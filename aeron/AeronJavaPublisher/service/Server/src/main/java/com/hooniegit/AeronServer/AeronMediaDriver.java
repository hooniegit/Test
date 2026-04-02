package com.hooniegit.AeronServer;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.springframework.stereotype.Component;

@Component
public class AeronMediaDriver {

    private final String aeronDir = System.getProperty("java.io.tmpdir") + "/aeron-sbe-ipc";

    static {
        // OS 공통 임시 폴더에 고정된 디렉토리 지정
        String aeronDir = System.getProperty("java.io.tmpdir") + "/aeron-sbe-ipc";
        System.out.println("Standalone Media Driver 시작됨... 디렉토리: " + aeronDir);

        MediaDriver.Context ctx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                // 스레드 모드를 공유로 설정하여 자원 소모 최적화
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true);

        // 드라이버 실행 (애플리케이션이 종료될 때까지 무한 대기)
        try (MediaDriver driver = MediaDriver.launch(ctx)) {
            System.out.println("Media Driver가 백그라운드에서 실행 중입니다. (Ctrl+C로 종료)");
            // 메인 스레드가 종료되지 않도록 블로킹
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.out.println("Media Driver 종료됨.");
        }
    }

}
