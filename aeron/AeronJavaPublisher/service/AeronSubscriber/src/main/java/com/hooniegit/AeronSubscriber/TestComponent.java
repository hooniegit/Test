package com.hooniegit.AeronSubscriber;

import com.hooniegit.sbe.ListDataMessageDecoder;
import com.hooniegit.sbe.SingleDataMessageDecoder;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

/**
 * 테스트용 컴포넌트입니다.
 */
@Component
public class TestComponent {

    private DataSubscriber subscriber = new DataSubscriber("aeron-sbe-ipc", 11);

//    private DataSubscriber subscriber = new DataSubscriber("aeron-sbe-ipc", 10,
//            "0.0.0.0", 1092);

    @PostConstruct
    private void start() {
        subscriber.startReceiving(new DataMessageListener() {
            @Override
            public void onSingleDataReceived(SingleDataMessageDecoder decoder) {
                int id = decoder.id();
                String value = decoder.value();
                String timestamp = decoder.timestamp();
                System.out.println("[단일 데이터 수신] ID: " + id + ", Value: " + value + ", Timestamp: " + timestamp);

            }

            @Override
            public void onListDataReceived(ListDataMessageDecoder decoder) {
                // 리스트 객체를 만들지 않고 디코더의 이터레이터를 통해 버퍼를 순회
                for (ListDataMessageDecoder.EntriesDecoder entry : decoder.entries()) {
                    int id = entry.id();
                    double value = entry.value();

                    // TODO: 읽어들인 id와 value를 즉시 비즈니스 로직에 반영 (배열 재사용 등)
                }

                // 💡 [중요] SBE 규칙: 가변 길이 데이터(String 등)는 반복문을 다 돈 '후'에 읽어야 합니다!
                String timestamp = decoder.timestamp();

                System.out.println("[리스트 데이터 처리 완료] 시간: " + timestamp);
            }
        });

        // 수신기가 백그라운드에서 동작하도록 메인 스레드 유지
        try {
            Thread.sleep(60000); // 1분간 수신
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        subscriber.stopReceiving();
    }

}
