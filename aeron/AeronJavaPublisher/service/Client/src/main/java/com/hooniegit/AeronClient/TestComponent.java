package com.hooniegit.AeronClient;

import com.hooniegit.sbe.ListDataMessageEncoder;
import com.hooniegit.sbe.MessageHeaderEncoder;
import com.hooniegit.sbe.SingleDataMessageEncoder;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;

import jakarta.annotation.PostConstruct;
import org.agrona.concurrent.UnsafeBuffer;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Component
public class TestComponent {

    private final List<TagData<Double>> dataList = new ArrayList<>();
    private final DataPublisher dataPublisher = new DataPublisher();

    @PostConstruct
    public void test() throws InterruptedException {
//        generate();

        this.dataPublisher.connect();
        Thread.sleep(3000);

        while (true) {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
            generateTagData();
            this.dataPublisher.publishListDataMessaage(dataList, timestamp);
            this.dataList.clear();
            Thread.sleep(1);
        }

    }

    public void generateTagData() {
        for (int i = 0; i < 10; i++) {
            this.dataList.add(new TagData<Double>(i, 100.0 + i));
        }
    }

    public void generate() {

        // 1. Aeron 설정 및 연결 (임베디드 Media Driver 사용)
        String aeronDir = System.getProperty("java.io.tmpdir") + "/aeron-sbe-ipc";
        MediaDriver.Context mediaDriverCtx = new MediaDriver.Context().aeronDirectoryName(aeronDir).dirDeleteOnStart(true);

        try (MediaDriver driver = MediaDriver.launch(mediaDriverCtx);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
             Publication publication = aeron.addPublication("aeron:ipc", 10)) {

            System.out.println("[Java Publisher] Aeron IPC 연결 완료. 데이터 전송 시작...");

            // 2. Zero-Allocation을 위한 버퍼 및 인코더 1회 생성 (재사용)
            UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(1024));
            MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
            SingleDataMessageEncoder singleEncoder = new SingleDataMessageEncoder();
            ListDataMessageEncoder listEncoder = new ListDataMessageEncoder();

            int counter = 1;

            while (true) {
                long result;

                /* =========================================================
                 * [메시지 1] SingleDataMessage 전송
                 * ========================================================= */
                singleEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);

                singleEncoder.id(counter)
                        .value(100.5 * counter)
                        .timestamp("2026-04-02 10:00:00.000"); // 내부적으로 UTF-8 바이트 복사 수행

                int singleMsgLength = MessageHeaderEncoder.ENCODED_LENGTH + singleEncoder.encodedLength();

                while ((result = publication.offer(buffer, 0, singleMsgLength)) < 0L) {
                    Thread.yield();
                }
                System.out.println("[Java] SingleDataMessage 전송 완료");

                /* =========================================================
                 * [메시지 2] ListDataMessage 전송
                 * ========================================================= */
                listEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);

                // 리스트(Group) 인코딩: 요소 개수(2개)를 지정하고 이터레이터를 가져옵니다.
                ListDataMessageEncoder.EntriesEncoder entries = listEncoder.entriesCount(2);

                // 첫 번째 리스트 아이템
                entries.next().id(counter * 10).value(10.1);
                // 두 번째 리스트 아이템
                entries.next().id(counter * 10 + 1).value(20.2);

                // 그룹(List) 인코딩이 끝난 직후에 가변 길이 문자열(String)을 인코딩해야 합니다! (순서 중요)
                listEncoder.timestamp("2026-04-02 10:00:01.000");

                int listMsgLength = MessageHeaderEncoder.ENCODED_LENGTH + listEncoder.encodedLength();

                while ((result = publication.offer(buffer, 0, listMsgLength)) < 0L) {
                    Thread.yield();
                }
                System.out.println("[Java] ListDataMessage 전송 완료");

                counter++;
                Thread.sleep(2000); // 2초 대기
            }
        } catch (Exception e) {}

    }

}
