package com.hooniegit.AeronSubscriber;
import com.hooniegit.sbe.ListDataMessageDecoder;
import com.hooniegit.sbe.MessageHeaderDecoder;
import com.hooniegit.sbe.SingleDataMessageDecoder;
import io.aeron.Aeron;
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

public class DataSubscriber {

    // Aeron
    private String aeronDir;
    private int streamId;
    private String channel;
    private Aeron aeron;
    private Subscription subscription;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread receiverThread;

    // SBE 디코더 객체 (수신 스레드 전용이므로 매번 새로 생성할 필요 없이 재사용합니다)
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final SingleDataMessageDecoder singleDecoder = new SingleDataMessageDecoder();
    private final ListDataMessageDecoder listDecoder = new ListDataMessageDecoder();

    // Logger
    private static final Logger log = LoggerFactory.getLogger(DataSubscriber.class);

    public DataSubscriber(final String location, int streamId) {
        this.aeronDir = System.getProperty("java.io.tmpdir") + "/" + location;
        this.streamId = streamId;
        this.channel = "aeron:ipc";
    }

    public DataSubscriber(final String location, int streamId, String publisherIp, int publisherPort) {
        this.aeronDir = System.getProperty("java.io.tmpdir") + "/" + location;
        this.streamId = streamId;
        this.channel = "aeron:udp?endpoint=" + publisherIp + ":" + publisherPort;
    }

    public void startReceiving(DataMessageListener listener) {
        this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        this.subscription = aeron.addSubscription(channel, streamId);
        this.running.set(true);

        log.info("[" + streamId + "] 스트림 수신 대기 중...");

        FragmentHandler fragmentHandler = (DirectBuffer buffer, int offset, int length, Header aeronHeader) -> {

            headerDecoder.wrap(buffer, offset);
            int templateId = headerDecoder.templateId();
            int actingBlockLength = headerDecoder.blockLength();
            int actingVersion = headerDecoder.version();
            int bodyOffset = offset + headerDecoder.encodedLength();

            if (templateId == singleDecoder.sbeTemplateId()) {

                // 1. 디코더를 버퍼의 데이터 위치로 이동 (포인터 세팅)
                singleDecoder.wrap(buffer, bodyOffset, actingBlockLength, actingVersion);

                // 2. 외부 콜백으로 디코더 자체를 전달 (Zero Allocation)
                listener.onSingleDataReceived(singleDecoder);

            } else if (templateId == listDecoder.sbeTemplateId()) {

                // 1. 디코더 매핑
                listDecoder.wrap(buffer, bodyOffset, actingBlockLength, actingVersion);

                // 2. 외부 콜백으로 전달
                listener.onListDataReceived(listDecoder);

            }
        };

        // 2. 조각난 패킷을 하나로 합쳐주는 Assembler로 Handler를 감싸줍니다 (C#의 assembler 역할)
        ImageFragmentAssembler assembler = new ImageFragmentAssembler(fragmentHandler);

        receiverThread = new Thread(() -> pollLoop(assembler));
        receiverThread.setName("Aeron-Receiver-Thread");
        receiverThread.start();
    }

    /**
     * C#과 동일하게 구성된 폴링 무한 루프
     */
    private void pollLoop(ImageFragmentAssembler assembler) {
        // C#의 idleStrategy.Idle()과 동일한 역할
        IdleStrategy idleStrategy = new SleepingIdleStrategy(1_000_000); // 1ms 대기

        // C# 코드와 동일하게 처리량 설정
        int pollCount = 1024;

        // 수신 무한 루프
        while (running.get()) {
            // raw handler 대신 assembler를 전달하여, 조립이 완료된 온전한 메시지만 handler로 넘어가게 합니다.
            int fragmentsRead = subscription.poll(assembler, pollCount);

            idleStrategy.idle(fragmentsRead);
        }
    }

    public void stopReceiving() {
        running.set(false);
        try {
            if (receiverThread != null) receiverThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (subscription != null) subscription.close();
        if (aeron != null) aeron.close();
    }
}