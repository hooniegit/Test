package com.hooniegit.AeronClient;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * 테스트용 컴포넌트입니다.
 */
@Component
public class TestComponent {

    private final List<TagData<Double>> dataList = new ArrayList<>();
    private final DataPublisher dataPublisher = new DataPublisher("aeron-sbe-ipc");

    @PostConstruct
    public void test() throws InterruptedException {
        this.dataPublisher.connect();
        Thread.sleep(3000);

        while (true) {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
            generateTagData();
            this.dataPublisher.publishListDataMessage(dataList, timestamp);
            this.dataList.clear();
            Thread.sleep(1);
        }

    }

    public void generateTagData() {
        for (int i = 0; i < 5000; i++) {
            this.dataList.add(new TagData<Double>(i, 100.0 + i));
        }
    }

}
