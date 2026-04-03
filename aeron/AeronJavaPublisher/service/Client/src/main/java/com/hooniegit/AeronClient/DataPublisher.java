package com.hooniegit.AeronClient;

import com.hooniegit.sbe.ListDataMessageEncoder;
import com.hooniegit.sbe.MessageHeaderEncoder;
import com.hooniegit.sbe.SingleDataMessageEncoder;
import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.List;

public class DataPublisher {

    private final String aeronDir = System.getProperty("java.io.tmpdir") + "/aeron-sbe-ipc";

    // Aeron과 Publication은 하나만 생성하여 모든 스레드가 공유 (Thread-safe)
    private Aeron aeron;
    private Publication publication;
    private volatile boolean isConnected = false;

    // 🌟 [핵심] 스레드별로 독립적인 버퍼와 인코더를 가지도록 ThreadLocal 사용
    private static final ThreadLocal<EncodingResources> TL_RESOURCES =
            ThreadLocal.withInitial(EncodingResources::new);

    // 스레드마다 하나씩 생성될 자원 컨테이너
    private static class EncodingResources {
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(1_048_576));
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final SingleDataMessageEncoder singleEncoder = new SingleDataMessageEncoder();
        final ListDataMessageEncoder listEncoder = new ListDataMessageEncoder();
    }

    // 싱글톤으로 사용하기 위한 기본 생성자
    public DataPublisher() {}

    public void connect() {
        try {
            this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
            this.publication = aeron.addPublication("aeron:ipc", 10);
            this.isConnected = true;
        } catch (Exception e) {
            e.printStackTrace();
            this.isConnected = false;
        }
    }

    public void publishSingleDataMessage(int id, double value, String timestamp) {
        if (!isConnected) {
            throw new IllegalStateException("Aeron is not connected.");
        }

        // 현재 실행 중인 스레드 전용 자원을 가져옴 (락 없이 Thread-safe 보장)
        EncodingResources resources = TL_RESOURCES.get();

        resources.singleEncoder.wrapAndApplyHeader(resources.buffer, 0, resources.headerEncoder);
        resources.singleEncoder.id(id)
                .value(value)
                .timestamp(timestamp);

        int msgLength = MessageHeaderEncoder.ENCODED_LENGTH + resources.singleEncoder.encodedLength();

        long result;
        while ((result = publication.offer(resources.buffer, 0, msgLength)) < 0L) {
            Thread.yield(); // 백프레셔 발생 시 양보
        }
    }

    public void publishListDataMessage(List<TagData<Double>> dataList, String timestamp) {
        if (!isConnected) {
            throw new IllegalStateException("Aeron is not connected.");
        }

        EncodingResources resources = TL_RESOURCES.get();

        resources.listEncoder.wrapAndApplyHeader(resources.buffer, 0, resources.headerEncoder);

        ListDataMessageEncoder.EntriesEncoder entries = resources.listEncoder.entriesCount(dataList.size());
        for (TagData<Double> data : dataList) {
            entries.next()
                    .id(data.getId())
                    .value(data.getValue());
        }
        resources.listEncoder.timestamp(timestamp);

        int msgLength = MessageHeaderEncoder.ENCODED_LENGTH + resources.listEncoder.encodedLength();

        long result;
        while ((result = publication.offer(resources.buffer, 0, msgLength)) < 0L) {
            Thread.yield();
        }
    }

    public void disconnect() {
        this.isConnected = false;
        if (publication != null) publication.close();
        if (aeron != null) aeron.close();

        // 메모리 릭 방지를 위해 현재 스레드의 로컬 자원 해제
        TL_RESOURCES.remove();
    }
}