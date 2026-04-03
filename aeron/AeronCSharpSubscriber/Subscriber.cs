using System;
using Com.Hooniegit.Sbe;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using Org.SbeTool.Sbe.Dll;

namespace Tools
{
    public class DataSubscriber : IDisposable
    {
        private readonly string _aeronDir;
        private Aeron _aeron;
        private Subscription _subscription;

        // 무한 루프를 안전하게 종료하기 위한 플래그
        private volatile bool _isRunning = false;

        // 외부에서 데이터를 처리할 수 있도록 콜백 선언
        public Action<ListDataMessage> OnListDataReceived { get; set; }
        public Action<SingleDataMessage> OnSingleDataReceived { get; set; }

        public DataSubscriber(string aeronDir = null)
        {
            // 경로를 주입받지 않으면 기본 임시 폴더 사용
            _aeronDir = aeronDir ?? System.IO.Path.GetTempPath() + "aeron-sbe-ipc";
        }

        public void Connect()
        {
            try
            {
                var ctx = new Aeron.Context().AeronDirectoryName(_aeronDir);
                _aeron = Aeron.Connect(ctx);
                _subscription = _aeron.AddSubscription("aeron:ipc", 10);

                Console.WriteLine("[.NET Subscriber] Aeron 연결 완료. 수신 준비 완료.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[.NET Subscriber] 연결 실패: {ex.Message}");
                throw;
            }
        }

        public void Start()
        {
            if (_subscription == null)
            {
                throw new InvalidOperationException("Aeron is not connected. Call Connect() first.");
            }

            _isRunning = true;

            // 핸들러를 FragmentAssembler로 래핑
            var myDataHandler = new FragmentHandler(OnFragmentReceived);
            var assembler = new FragmentAssembler(myDataHandler);

            var idleStrategy = new SleepingIdleStrategy(1); // 평시용
            // var idleStrategy = new BusySpinIdleStrategy(); // 초고성능용

            Console.WriteLine("[.NET Subscriber] 데이터 수신 루프를 시작합니다...");

            // 수신 무한 루프
            while (_isRunning)
            {
                int pollCount = 1024;
                int fragmentsRead = _subscription.Poll(assembler.OnFragment, pollCount);
                idleStrategy.Idle(fragmentsRead);
            }
        }

        public void Stop()
        {
            _isRunning = false;
            Console.WriteLine("[.NET Subscriber] 수신 루프 종료 요청됨.");
        }

        // 기존 람다 함수 내부 로직을 독립된 메서드로 추출
        // 네임스페이스 충돌 현상 방지를 위해 풀네임 기입
        private void OnFragmentReceived(Adaptive.Agrona.IDirectBuffer buffer, int offset, int length, Header aeronHeader)
        {
            // 1. 페이로드 복사 및 버퍼 래핑
            byte[] payload = new byte[length];
            buffer.GetBytes(offset, payload, 0, length);
            var sbeBuffer = new DirectBuffer(payload);

            // 2. 메시지 헤더 디코딩
            var msgHeader = new MessageHeader();
            msgHeader.Wrap(sbeBuffer, 0, 0);

            int templateId = msgHeader.TemplateId;
            int blockLength = msgHeader.BlockLength;
            int version = msgHeader.Version;
            int messageOffset = MessageHeader.Size;

            // 3. 메시지 템플릿별 분기 처리
            if (templateId == SingleDataMessage.TemplateId)
            {
                var singleMsg = new SingleDataMessage();
                singleMsg.WrapForDecode(sbeBuffer, messageOffset, blockLength, version);

                // 현장 배포 시 제외할 로그 (주석 처리됨)
                // Console.WriteLine("=== [수신: SingleDataMessage] ===");
                // Console.WriteLine($"ID: {singleMsg.Id}, Value: {singleMsg.Value}");

                OnSingleDataReceived?.Invoke(singleMsg);

            }
            else if (templateId == ListDataMessage.TemplateId)
            {
                var listMsg = new ListDataMessage();
                listMsg.WrapForDecode(sbeBuffer, messageOffset, blockLength, version);

                OnListDataReceived?.Invoke(listMsg);
            }
        }


        // ListDataMessage.GetTimestamp() : string
        // ListDataMessage.Entries : 

        // IDisposable 인터페이스 구현 (자원 안전 해제)
        public void Dispose()
        {
            Stop();

            if (_subscription != null)
            {
                _subscription.Dispose();
                _subscription = null;
            }

            if (_aeron != null)
            {
                _aeron.Dispose();
                _aeron = null;
            }

            Console.WriteLine("[.NET Subscriber] 자원 해제 완료.");
        }
    }
}