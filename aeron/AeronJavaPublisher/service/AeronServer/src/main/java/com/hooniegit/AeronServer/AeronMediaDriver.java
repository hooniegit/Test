package com.hooniegit.AeronServer;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.io.File;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Aeron Media Driver 클래스를 Spring Bean 환경에 등록 및 관리합니다.
 * 애플리케이션 시작 시 미디어 드라이버를 자동으로 시작하고, 종료 시 안전하게 종료합니다.
 * 또한, 주기적으로 드라이버 상태를 확인하여 비정상일 경우 자동으로 재시작합니다.
 */
@Component
public class AeronMediaDriver {

    // Media Driver
    @Value("${aeron.server.location:aeron-sbe-ipc}")
    private String location;
    private String aeronDir;
    private MediaDriver mediaDriver;

    // Lock
    private final ReentrantLock lock = new ReentrantLock();

    // Logger
    private static final Logger log = LoggerFactory.getLogger(AeronMediaDriver.class);

    @PostConstruct
    public void init() {
        this.aeronDir = System.getProperty("java.io.tmpdir") + location;
        startDriver();
    }

    @PreDestroy
    public void destroy() {
        stopDriver();
    }

    /**
     * 미디어 드라이버를 시작합니다.
     */
    private void startDriver() {
        // 동시 접근 방지
        lock.lock();

        try {
            // 이미 실행 중이면 무시
            if (mediaDriver != null) {
                return;
            }

            MediaDriver.Context ctx = new MediaDriver.Context()
                    .aeronDirectoryName(aeronDir)
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    // 드라이버 내부에서 발생하는 에러를 감지하기 위한 핸들러 추가
                    .errorHandler(throwable -> log.error("[Standalone] 미디어 드라이버 내부 오류 발생", throwable));

            mediaDriver = MediaDriver.launch(ctx);
            log.info("[Standalone] 미디어 드라이버가 시작되었습니다. 디렉토리: {}", aeronDir);

        } catch (Exception e) {
            log.error("[Standalone] 미디어 드라이버 시작 실패", e);
        } finally {
            // 락 해제
            lock.unlock();
        }
    }

    /**
     * 미디어 드라이버를 안전하게 종료합니다.
     */
    private void stopDriver() {
        lock.lock();
        try {
            if (mediaDriver != null) {
                mediaDriver.close();
                mediaDriver = null;
                log.info("[Standalone] 미디어 드라이버가 종료되었습니다.");
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 주기적으로 드라이버 상태를 확인하고, 비정상일 경우 재시작합니다.
     * 'aeron.server.monitoring.interval' 속성으로 스케줄링 간격을 설정할 수 있습니다(기본값: 10000ms).
     */
    @Scheduled(fixedDelayString = "${aeron.server.monitoring.interval:10000}")
    public void checkHealthAndRestart() {
        boolean isHealthy = false;

        if (mediaDriver != null) {
            // OS의 tmp 폴더 정리 작업 등으로 인해 Aeron 디렉토리가 삭제되었는지 확인
            File dir = new File(aeronDir);
            if (dir.exists() && dir.isDirectory()) {
                // CnC(Command and Control) 파일이 정상적으로 존재하는지 추가 확인
                File cncFile = new File(dir, "cnc.dat");
                if (cncFile.exists()) {
                    isHealthy = true;
                }
            }
        }

        if (!isHealthy) {
            log.warn("[Standalone] 미디어 드라이버가 비정상 상태입니다. 재시작을 시도합니다...");
            stopDriver();
            startDriver();
        } else {
            log.debug("[Standalone] 미디어 드라이버 정상 작동 중...");
        }
    }

}