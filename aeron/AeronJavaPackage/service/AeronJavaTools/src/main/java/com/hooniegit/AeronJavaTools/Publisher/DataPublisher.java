package com.hooniegit.AeronJavaTools.Publisher;

import com.hooniegit.AeronJavaTools.Common.TagData;
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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Aeron 미디어 드라이버와 연결하여 SBE(Simple Binary Encoding)로 인코딩된 메시지를 IPC 채널을 통해 전송합니다.
 * @Warning 다중 스레드 환경의 경우, connect() 를 수행한 단일 객체를 공유하는 방식으로 사용해야 합니다.
 */
public class DataPublisher {

    // Aeron
    private String aeronDir;
    private int streamId;
    private String channel;
    private Aeron aeron;
    private Publication publication;
    private volatile boolean isConnected = false;
    private final AtomicBoolean running = new AtomicBoolean(true);

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

    // IPC 전용 생성자
    public DataPublisher(String location, int streamId) {
        this.aeronDir = System.getProperty("java.io.tmpdir") + "/" + location;
        this.streamId = streamId;
        this.channel = "aeron:ipc";
    }

    // UDP 전용 생성자
    public DataPublisher(String location, int streamId, String targetIp, int targetPort) {
        this.aeronDir = System.getProperty("java.io.tmpdir") + "/" + location;
        this.streamId = streamId;
        this.channel = "aeron:udp?endpoint=" + targetIp + ":" + targetPort;
    }

    /**
     * 미디어 드라이버에 연결합니다.
     */
    public void connect() {
        try {
            // 이전 할당 자원 제거
            if (publication != null && !publication.isClosed()) {
                publication.close();
            }
            if (aeron != null && !aeron.isClosed()) {
                aeron.close();
            }

            Aeron.Context ctx = new Aeron.Context()
                    .aeronDirectoryName(aeronDir)
                    .errorHandler(this::onError);

            this.aeron = Aeron.connect(ctx);
            this.publication = aeron.addPublication(channel, streamId);
            this.isConnected = true;
        } catch (Exception e) {
            log.warn("[Java Publisher: {}] 미디어 드라이버 연결 실패", streamId, e);
            this.isConnected = false;
        }
    }

    private void onError(Throwable throwable) {
        log.error("[Java Publisher: {}] Aeron 내부 에러 발생: {}", streamId, throwable.getMessage());
        this.isConnected = false; // 드라이버 통신 장애 시 연결 상태 false
    }

    /**
     * 단일 데이터 메시지를 SBE로 인코딩하여 전송합니다.
     * @param id Tag ID
     * @param value Tag Value
     * @param timestamp Tag Timestamp (예: "2024-06-01T12:00:00.000")
     */
    public void publishSingleDataMessage(int id, String value, String timestamp) {
        if (!isConnected || publication.isClosed()) {
            log.warn("[Java Publisher: {}] 전송 실패: 미디어 드라이버 연결 확인 필요", streamId);
            connect();

            // 재연결을 시도했는데도 여전히 실패했다면 그때 데이터를 버리고 빠져나옵니다.
            if (!isConnected) {
                log.warn("[Java Publisher: {}] 재연결 실패. 데이터를 전송하지 못했습니다.", streamId);
                return;
            }
        }

        // 현재 실행 중인 스레드 전용 자원 사용 (Lock (X) & Thread-safe 보장)
        EncodingResources resources = TL_RESOURCES.get();

        // SBE 인코딩: 헤더 + 메시지
        resources.singleEncoder.wrapAndApplyHeader(resources.buffer, 0, resources.headerEncoder);
        resources.singleEncoder.id(id)
                .value(value)
                .timestamp(timestamp);

        // 메세지 전송
        long result;
        int msgLength = MessageHeaderEncoder.ENCODED_LENGTH + resources.singleEncoder.encodedLength();
        int retries = 0;
        long notConnectedCount = 0; // 로그 스팸 방지용 카운터

        while ((result = publication.offer(resources.buffer, 0, msgLength)) < 0L) {
            if (result == Publication.NOT_CONNECTED) {
                if (notConnectedCount++ % 1000 == 0) {
                    log.warn("[Java Publisher: {}] 구독자가 없습니다. 연결 대기 중...", streamId);
                }

                // CPU 100% 점유를 막기 위해 아주 짧게(1ms) 스레드를 재웁니다.
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // 인터럽트 발생 시 스레드 종료 신호 복구
                    log.error("[Java Publisher: {}] 대기 중 인터럽트 발생. 전송을 취소합니다.", streamId);
                    break;
                }

            } else if (result == Publication.CLOSED) {
                log.error("[Java Publisher: {}] Publication이 닫혔습니다.", streamId);
                this.isConnected = false;
                break;
            } else if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
                // 수신단이 너무 느림
                retries++;
                if (retries > 1000) {
                    log.warn("[Java Publisher: {}] 백프레셔 지속으로 전송 포기", streamId);
                    break;
                }
                Thread.yield(); // 백프레셔 상황에서는 Sleep 대신 Yield가 성능에 유리합니다.
            }
        }
    }

    /**
     * 리스트 데이터 메시지를 SBE로 인코딩하여 전송합니다.
     * @param dataList TagData 객체 리스트
     * @param timestamp 리스트 내 TagData 공통 Timestamp (예: "2024-06-01T12:00:00.000")
     */
    public void publishListDataMessage(List<TagData<Double>> dataList, String timestamp) {
        if (!isConnected || publication.isClosed()) {
            log.warn("[Java Publisher: {}] 전송 실패: 미디어 드라이버 연결 확인 필요", streamId);
            connect();

            // 재연결을 시도했는데도 여전히 실패했다면 그때 데이터를 버리고 빠져나옵니다.
            if (!isConnected) {
                log.warn("[Java Publisher: {}] 재연결 실패. 데이터를 전송하지 못했습니다.", streamId);
                return;
            }
        }

        // 현재 실행 중인 스레드 전용 자원 사용 (Lock (X) & Thread-safe 보장)
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
        int retries = 0;
        long notConnectedCount = 0; // 로그 스팸 방지용 카운터

        if (msgLength > publication.maxMessageLength()) {
            log.error("[Java Publisher: {}] 메시지 크기({} bytes)가 Aeron 허용 한계({})를 초과했습니다. 리스트 분할 전송이 필요합니다.",
                    streamId, msgLength, publication.maxMessageLength());
            return;
        }

        while ((result = publication.offer(resources.buffer, 0, msgLength)) < 0L) {
            if (result == Publication.NOT_CONNECTED) {
                if (notConnectedCount++ % 1000 == 0) {
                    log.warn("[Java Publisher: {}] 구독자가 없습니다. 연결 대기 중...", streamId);
                }

                // CPU 100% 점유를 막기 위해 아주 짧게(1ms) 스레드를 재웁니다.
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // 인터럽트 발생 시 스레드 종료 신호 복구
                    log.error("[Java Publisher: {}] 대기 중 인터럽트 발생. 전송을 취소합니다.", streamId);
                    break;
                }

            } else if (result == Publication.CLOSED) {
                log.error("[Java Publisher: {}] Publication이 닫혔습니다.", streamId);
                this.isConnected = false;
                break;

            } else if (result == Publication.MAX_POSITION_EXCEEDED) {
                log.error("[Java Publisher: {}] Publication 포지션 한계에 도달했습니다. 연결을 재시작해야 합니다.", streamId);
                this.isConnected = false;
                break;
            } else if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
                retries++;
                if (retries > 1000) {
                    log.warn("[Java Publisher: {}] 백프레셔 지속으로 리스트 데이터 전송 포기", streamId);
                    break;
                }
                Thread.yield();
            }
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