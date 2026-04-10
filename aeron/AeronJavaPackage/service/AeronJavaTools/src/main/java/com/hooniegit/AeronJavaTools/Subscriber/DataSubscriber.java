package com.hooniegit.AeronJavaTools.Subscriber;

import com.hooniegit.sbe.ListDataMessageDecoder;
import com.hooniegit.sbe.MessageHeaderDecoder;
import com.hooniegit.sbe.SingleDataMessageDecoder;
import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Aeron 미디어 드라이버와 연결하여 SBE(Simple Binary Encoding)로 인코딩된 메시지를 IPC 또는 UDP 채널을 통해 수신합니다.
 * @Warning 다중 스레드 환경의 경우, connect() 를 수행한 단일 객체를 공유하는 방식으로 사용해야 합니다.
 */
public class DataSubscriber implements AutoCloseable {

    // Aeron
    private String aeronDir;
    private int streamId;
    private String channel;
    private Aeron aeron;
    private Subscription subscription;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread receiverThread;

    // 외부 콜백용 Runnable
    private Runnable onConnected;
    private Runnable onDisconnected;

    // SBE 디코더 객체 (수신 스레드 전용이므로 매번 새로 생성할 필요 없이 재사용합니다)
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final SingleDataMessageDecoder singleDecoder = new SingleDataMessageDecoder();
    private final ListDataMessageDecoder listDecoder = new ListDataMessageDecoder();

    // Logger
    private static final Logger log = LoggerFactory.getLogger(DataSubscriber.class);

    // IPC 전용 생성자
    public DataSubscriber(final String aeronDir, int streamId) {
        this.aeronDir = System.getProperty("java.io.tmpdir") + "/" + aeronDir;
        this.streamId = streamId;
        this.channel = "aeron:ipc";
    }

    // UDP 전용 생성자
    public DataSubscriber(final String location, int streamId, String targetIp, int targetPort) {
        this.aeronDir = System.getProperty("java.io.tmpdir") + "/" + location;
        this.streamId = streamId;
        this.channel = "aeron:udp?endpoint=" + targetIp + ":" + targetPort;
    }

    /**
     * 미디어 드라이버와 연결합니다.
     */
    public void connect() {
        Aeron.Context ctx = new Aeron.Context()
                .aeronDirectoryName(aeronDir)
                .availableImageHandler(this::onAvailableImage)
                .unavailableImageHandler(this::onUnavailableImage)
                .errorHandler(this::onError); // 백그라운드 스레드 에러 방어

        this.aeron = Aeron.connect(ctx);
        this.subscription = aeron.addSubscription(channel, streamId);
        log.info("[Java Subscriber: {}] Aeron 미디어 드라이버 접속 완료. 채널: {}", streamId, channel);
    }

    /**
     * 이미지 연결/끊김 이벤트 핸들러
     */
    private void onAvailableImage(Image image) {
        log.info("[Java Subscriber: {}] 연결 감지됨 (SessionId: {})", streamId, image.sessionId());
        if (onConnected != null) onConnected.run();
    }

    /**
     * 이미지 연결 끊김 이벤트 핸들러
     */
    private void onUnavailableImage(Image image) {
        log.warn("[Java Subscriber: {}] 연결 끊어짐 감지 (SessionId: {})", streamId, image.sessionId());
        if (onDisconnected != null) onDisconnected.run();
    }

    /**
     * Aeron 내부 Exception 발생 이벤트 핸들러
     * @param ex
     */
    private void onError(Throwable ex) {
        log.error("[Java Subscriber: {}] Aeron 에러 감지: {}", streamId, ex.getMessage());
        stopReceiving(); // 루프 강제 종료
        if (onDisconnected != null) onDisconnected.run();
    }

    public void setOnConnected(Runnable onConnected) { this.onConnected = onConnected; }
    public void setOnDisconnected(Runnable onDisconnected) { this.onDisconnected = onDisconnected; }

    /**
     * 미디어 드라이버에 연결하여 메시지 수신을 시작합니다.
     * @param listener
     */
    public void startReceiving(DataMessageListener listener) {
        if (subscription == null) throw new IllegalStateException("connect()를 먼저 호출하세요.");

        this.running.set(true);

        FragmentHandler fragmentHandler = (DirectBuffer buffer, int offset, int length, Header aeronHeader) -> {
            headerDecoder.wrap(buffer, offset);
            int templateId = headerDecoder.templateId();
            int actingBlockLength = headerDecoder.blockLength();
            int actingVersion = headerDecoder.version();
            int bodyOffset = offset + headerDecoder.encodedLength();

            if (templateId == singleDecoder.sbeTemplateId()) {
                singleDecoder.wrap(buffer, bodyOffset, actingBlockLength, actingVersion);
                listener.onSingleDataReceived(singleDecoder);
            } else if (templateId == listDecoder.sbeTemplateId()) {
                listDecoder.wrap(buffer, bodyOffset, actingBlockLength, actingVersion);
                listener.onListDataReceived(listDecoder);
            }
        };

        // 조립이 완료된 온전한 메시지만 handler에서 처리
        ImageFragmentAssembler assembler = new ImageFragmentAssembler(fragmentHandler);
        IdleStrategy idleStrategy = new SleepingIdleStrategy(1_000_000); // 1ms 대기
        int pollCount = 1024;

        log.info("[Java Subscriber: {}] 데이터 수신 루프 시작...", streamId);

        // 무한 수신 루프
        while (running.get()) {
            int fragmentsRead = subscription.poll(assembler, pollCount);
            idleStrategy.idle(fragmentsRead);
        }

        log.info("[Java Subscriber: {}] 데이터 수신 루프 종료.", streamId);
    }

    /**
     * 수신 루프를 종료합니다.
     */
    public void stopReceiving() {
        this.running.set(false);
    }

    /**
     * C#과 동일하게 구성된 폴링 무한 루프
     */
    private void pollLoop(ImageFragmentAssembler assembler) {
        // C#의 idleStrategy.Idle()과 동일한 역할
        IdleStrategy idleStrategy = new SleepingIdleStrategy(1_000_000); // 1ms 대기

        // C# 코드와 동일하게 처리량 설정 (1MB)
        int pollCount = 1024;

        // 수신 무한 루프
        while (running.get()) {
            // raw handler 대신 assembler를 전달하여, 조립이 완료된 온전한 메시지만 handler로 넘어가게 합니다.
            int fragmentsRead = subscription.poll(assembler, pollCount);

            idleStrategy.idle(fragmentsRead);
        }
    }

    /**
     * 자원 해제
     */
    @Override
    public void close() {
        stopReceiving();
        if (subscription != null) subscription.close();
        if (aeron != null) aeron.close();
        log.info("[Java Subscriber: {}] DataSubscriber 자원 해제 완료.", streamId);
    }

}