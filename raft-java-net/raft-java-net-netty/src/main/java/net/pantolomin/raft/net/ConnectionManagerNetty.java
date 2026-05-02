package net.pantolomin.raft.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.pantolomin.raft.api.ConnectionManager;
import net.pantolomin.raft.api.RequestHandler;
import net.pantolomin.raft.domain.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class ConnectionManagerNetty implements ConnectionManager {
    private static final CompletableFuture NOT_CONNECTED_FUTURE = CompletableFuture.failedFuture(new NotConnectedException());

    private final Cluster cluster;
    private final ClusterMember localMember;
    private final Set<RequestHandler> subscribers = new CopyOnWriteArraySet<>();
    private final IoHandlerFactory ioHandlerFactory = NioIoHandler.newFactory();
    private ScheduledExecutorService scheduler;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture serverChannel;
    private Map<ClusterMember, MemberConnection> clientConnections = Map.of();

    @Override
    public void start() {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "scheduler-" + localMember.getId()));
        ExecutorService executor = Executors.newSingleThreadExecutor(
                r -> new Thread(r, "server-boss-" + localMember.getId())
        );
        this.bossGroup = new SingleThreadIoEventLoop(null, executor, ioHandlerFactory);
        executor = Executors.newSingleThreadExecutor(
                r -> new Thread(r, "server-worker-" + localMember.getId())
        );
        this.workerGroup = new SingleThreadIoEventLoop(null, executor, ioHandlerFactory);
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(this.bossGroup, this.workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new ObjectEncoder())
                                .addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)))
                                .addLast(new ServerInbound());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        // Bind and start to accept incoming connections
        this.serverChannel = bootstrap.bind(this.localMember.getHostname(), this.localMember.getPort());

        this.clientConnections = Arrays.stream(this.cluster.getMembers())
                .filter(m -> m != this.localMember)
                .collect(Collectors.toMap(Function.identity(), MemberConnection::new));
        this.clientConnections.forEach((m, c) -> c.start());
    }

    @Override
    public void stop() {
        this.clientConnections.forEach((m, c) -> c.stop());
        try {
            Channel channel = this.serverChannel.channel();
            channel.close().awaitUninterruptibly();
        } finally {
            this.workerGroup.shutdownGracefully();
            this.bossGroup.shutdownGracefully();
            this.scheduler.shutdownNow();
        }
    }

    @Override
    public void subscribe(RequestHandler handler) {
        this.subscribers.add(handler);
    }

    @Override
    public void unsubscribe(RequestHandler handler) {
        this.subscribers.remove(handler);
    }

    @Override
    public CompletionStage<AppendEntries.Response> send(ClusterMember remoteMember, AppendEntries appendEntries) {
        MemberConnection connection = this.clientConnections.get(remoteMember);
        if (connection != null) {
            return connection.send(appendEntries);
        }
        return NOT_CONNECTED_FUTURE;
    }

    @Override
    public CompletionStage<RequestVote.Response> send(ClusterMember remoteMember, RequestVote request) {
        MemberConnection connection = this.clientConnections.get(remoteMember);
        if (connection != null) {
            return connection.send(request);
        }
        return NOT_CONNECTED_FUTURE;
    }

    @RequiredArgsConstructor
    private final class MemberConnection {
        private final ClusterMember remoteMember;
        private ClientInbound clientInbound = new ClientInbound();
        private EventLoopGroup workerGroup;
        private ChannelFuture clientChannel;
        private ScheduledFuture<?> reconnectionTask;

        void start() {
            ExecutorService executor = Executors.newSingleThreadExecutor(
                    r -> new Thread(r, "client-worker-" + localMember.getId() + "-" + remoteMember.getId())
            );
            this.workerGroup = new SingleThreadIoEventLoop(null, executor, ioHandlerFactory);
            connect();
            this.reconnectionTask = scheduler.scheduleAtFixedRate(() -> this.workerGroup.submit(() -> {
                if (this.clientInbound.ctx == null) {
                    this.clientChannel.channel().close().awaitUninterruptibly();
                    this.clientInbound = new ClientInbound();
                    connect();
                }
            }), 1L, 1L, TimeUnit.SECONDS);
        }

        void stop() {
            try {
                this.workerGroup.submit(() -> {
                    this.reconnectionTask.cancel(false);
                    this.clientChannel.channel().close().awaitUninterruptibly();
                }).awaitUninterruptibly();
            } finally {
                this.workerGroup.shutdownGracefully();
            }
        }

        private void connect() {
            Bootstrap bootstrap = new Bootstrap()
                    .group(this.workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ObjectEncoder())
                                    .addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)))
                                    .addLast(clientInbound);
                        }
                    });
            this.clientChannel = bootstrap.connect(this.remoteMember.getHostname(), this.remoteMember.getPort());
        }

        public CompletionStage<AppendEntries.Response> send(AppendEntries appendEntries) {
            return CompletableFuture.supplyAsync(() -> this.clientInbound.send(appendEntries), this.workerGroup)
                    .thenCompose(Function.identity());
        }

        public CompletionStage<RequestVote.Response> send(RequestVote request) {
            return CompletableFuture.supplyAsync(() -> this.clientInbound.send(request), this.workerGroup)
                    .thenCompose(Function.identity());
        }

        @RequiredArgsConstructor
        private final class ClientInbound extends ChannelInboundHandlerAdapter {
            private final AtomicReference<CompletableFuture<AppendEntries.Response>> appendFuture = new AtomicReference<>();
            private final AtomicReference<CompletableFuture<RequestVote.Response>> voteFuture = new AtomicReference<>();
            private ChannelHandlerContext ctx;

            @Override
            public void channelActive(ChannelHandlerContext ctx) {
                this.ctx = ctx;
                this.ctx.writeAndFlush(localMember.getHostname() + ":" + localMember.getPort());
                log.info("[{}] Client connection established for member {}", localMember.getId(), remoteMember.getId());
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx) {
                this.ctx = null;
                log.info("[{}] Client connection stopped for member {}", localMember.getId(), remoteMember.getId());
            }

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof AppendEntries.Response a) {
                    CompletableFuture<AppendEntries.Response> future = appendFuture.getAndSet(null);
                    if (future != null) {
                        future.complete(a);
                    } else {
                        log.warn("[{}] Received append response from {} despite not waiting for one", localMember.getId(), remoteMember.getId());
                    }
                } else if (msg instanceof RequestVote.Response v) {
                    CompletableFuture<RequestVote.Response> future = voteFuture.getAndSet(null);
                    if (future != null) {
                        future.complete(v);
                    } else {
                        log.warn("[{}] Received vote response from {} despite not waiting for one", localMember.getId(), remoteMember.getId());
                    }
                } else {
                    throw new IllegalStateException("Unexpected message received: " + msg.getClass());
                }
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                log.error("Inbound error -> restarting client", cause);
                stop();
                start();
            }

            public CompletionStage<AppendEntries.Response> send(AppendEntries appendEntries) {
                ChannelHandlerContext context = this.ctx;
                if (context == null) {
                    return NOT_CONNECTED_FUTURE;
                }
                CompletableFuture<AppendEntries.Response> future = new CompletableFuture<>();
                this.appendFuture.set(future);
                context.writeAndFlush(appendEntries);
                return future;
            }

            public CompletionStage<RequestVote.Response> send(RequestVote request) {
                ChannelHandlerContext context = this.ctx;
                if (context == null) {
                    return NOT_CONNECTED_FUTURE;
                }
                CompletableFuture<RequestVote.Response> future = new CompletableFuture<>();
                this.voteFuture.set(future);
                context.writeAndFlush(request);
                return future;
            }
        }
    }

    @RequiredArgsConstructor
    private final class ServerInbound extends ChannelInboundHandlerAdapter {
        private ClusterMember remoteMember;
        private ChannelHandlerContext ctx;

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            this.ctx = null;
            log.info("[{}] Server connection stopped for member {}", localMember.getId(), this.remoteMember.getId());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof String s) {
                Arrays.stream(cluster.getMembers())
                        .filter(m -> s.equals(m.getHostname() + ":" + m.getPort()))
                        .findFirst()
                        .ifPresentOrElse(
                                m -> {
                                    this.remoteMember = m;
                                    log.info("[{}] Server connection accepted for member {}: {}", localMember.getId(), m.getId(), s);
                                },
                                () -> log.warn("Received unknown member identification data: {}", s)
                        );
            } else {
                if (this.remoteMember == null) {
                    throw new IllegalStateException("Did not receive member identification data");
                }
                if (msg instanceof AppendEntries appendEntries) {
                    subscribers.forEach(h -> h.handle(this.remoteMember, appendEntries)
                            .thenAccept(this::sendResponse)
                    );
                } else if (msg instanceof RequestVote requestVote) {
                    subscribers.forEach(h -> h.handle(this.remoteMember, requestVote)
                            .thenAccept(this::sendResponse)
                    );
                } else {
                    throw new IllegalStateException("Unexpected message received: " + msg.getClass());
                }
            }
        }

        private <R> void sendResponse(R response) {
            workerGroup.submit(() -> {
                if (this.ctx != null) {
                    this.ctx.writeAndFlush(response);
                }
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Inbound error -> restarting server", cause);
            stop();
            start();
        }
    }
}
