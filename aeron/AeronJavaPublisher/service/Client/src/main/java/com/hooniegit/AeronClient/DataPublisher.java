package com.hooniegit.AeronClient;

import com.hooniegit.sbe.ListDataMessageEncoder;
import com.hooniegit.sbe.MessageHeaderEncoder;
import com.hooniegit.sbe.SingleDataMessageEncoder;
import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Aeron 미디어 드라이버와 연결하여 SBE(Simple Binary Encoding)로 인코딩된 메시지를 IPC 채널을 통해 전송합니다.
 * @Warning 다중 스레드 환경의 경우, connect() 를 수행한 단일 객체를 공유하는 방식으로 사용해야 합니다.
 */
public class DataPublisher {

    // Aeron
    private String aeronDir;
    // (Thread-safe)
    private Aeron aeron;
    private Publication publication;
    private volatile boolean isConnected = false;

    // ThreadLocal: 스레드별로 독립적인 버퍼와 인코더 할당
    private static final ThreadLocal<EncodingResources> TL_RESOURCES =
            ThreadLocal.withInitial(EncodingResources::new);

    // Resource Container: 스레드마다 독립적으로 사용할 버퍼와 인코더를 묶어서 관리
    private static class EncodingResources {
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(1_048_576)); // 1MB
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final SingleDataMessageEncoder singleEncoder = new SingleDataMessageEncoder();
        final ListDataMessageEncoder listEncoder = new ListDataMessageEncoder();
    }

    // Logger
    private static final Logger log = LoggerFactory.getLogger(DataPublisher.class);

    public DataPublisher(String location) {
        this.aeronDir = System.getProperty("java.io.tmpdir") + "/" + location;
    }

    /**
     * 미디어 드라이버에 연결합니다.
     */
    public void connect() {
        try {
            this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
            this.publication = aeron.addPublication("aeron:ipc", 10);
            this.isConnected = true;
        } catch (Exception e) {
            log.warn("미디어 드라이버 연결 실패", e);
            this.isConnected = false;
        }
    }

    /**
     * 단일 데이터 메시지를 SBE로 인코딩하여 전송합니다.
     * @param id
     * @param value
     * @param timestamp
     */
    public void publishSingleDataMessage(int id, String value, String timestamp) {
        if (!isConnected) {
            throw new IllegalStateException("미디어 드라이버 연결 필요");
        }

        // 현재 실행 중인 스레드 전용 자원을 가져옴 (락 없이 Thread-safe 보장)
        EncodingResources resources = TL_RESOURCES.get();

        // SBE 인코딩: 헤더 + 메시지
        resources.singleEncoder.wrapAndApplyHeader(resources.buffer, 0, resources.headerEncoder);
        resources.singleEncoder.id(id)
                .value(value)
                .timestamp(timestamp);

        // 메세지 전송
        long result;
        int msgLength = MessageHeaderEncoder.ENCODED_LENGTH + resources.singleEncoder.encodedLength();
        while ((result = publication.offer(resources.buffer, 0, msgLength)) < 0L) {
            Thread.yield(); // 백프레셔 발생 시 양보 (중요)
        }
    }

    /**
     * 리스트 데이터 메시지를 SBE로 인코딩하여 전송합니다.
     * @param dataList
     * @param timestamp
     */
    public void publishListDataMessage(List<TagData<Double>> dataList, String timestamp) {
        if (!isConnected) {
            throw new IllegalStateException("미디어 드라이버 연결 필요");
        }

        // 현재 실행 중인 스레드 전용 자원을 가져옴 (락 없이 Thread-safe 보장)
        EncodingResources resources = TL_RESOURCES.get();

        // SBE 인코딩: 헤더 + 메시지
        resources.listEncoder.wrapAndApplyHeader(resources.buffer, 0, resources.headerEncoder);
        ListDataMessageEncoder.EntriesEncoder entries = resources.listEncoder.entriesCount(dataList.size());
        for (TagData<Double> data : dataList) {
            entries.next()
                    .id(data.getId())
                    .value(data.getValue());
        }
        resources.listEncoder.timestamp(timestamp);

        // 메세지 전송
        long result;
        int msgLength = MessageHeaderEncoder.ENCODED_LENGTH + resources.listEncoder.encodedLength();
        while ((result = publication.offer(resources.buffer, 0, msgLength)) < 0L) {
            Thread.yield(); // 백프레셔 발생 시 양보 (중요)
        }
    }

    /**
     * 미디어 드라이버와의 연결을 종료하고 자원을 해제합니다.
     */
    public void disconnect() {
        this.isConnected = false;
        if (publication != null) publication.close();
        if (aeron != null) aeron.close();

        // 메모리 릭 방지를 위해 현재 스레드의 로컬 자원 해제
        TL_RESOURCES.remove();
    }
}