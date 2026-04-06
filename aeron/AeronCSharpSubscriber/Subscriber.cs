using System;
using Com.Hooniegit.Sbe;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using Org.SbeTool.Sbe.Dll;
using Microsoft.Extensions.Logging;

namespace Tools
{
    /// <summary>
    /// Aeron을 통해 SBE 데이터를 수신하는 Subscriber 클래스
    /// </summary>
    public class DataSubscriber : IDisposable
    {
        // Aeron
        private readonly string _aeronDir;
        private Aeron _aeron;
        private Subscription _subscription;
        private readonly int _streamId;

        // 동적으로 할당될 채널 (aeron:ipc 또는 aeron:udp)
        private readonly string _channel;

        // Flag: 무한 루프의 안전 종료
        private volatile bool _isRunning = false;

        // Action: 외부 Callback 호출
        public Action<ListDataMessage> OnListDataReceived { get; set; }
        public Action<SingleDataMessage> OnSingleDataReceived { get; set; }

        // Logging
        private static readonly ILoggerFactory LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
            builder.SetMinimumLevel(LogLevel.Debug);
        });
        private static readonly ILogger Logger = LoggerFactory.CreateLogger<DataSubscriber>();

        public DataSubscriber(string aeronDir = null, int streamId = 0, string targetIp = null, int targetPort = -1)
        {
            _aeronDir = aeronDir ?? System.IO.Path.GetTempPath() + "aeron-sbe-ipc";
            _streamId = streamId;

            // IP와 Port가 유효하게 전달되었는지 확인하여 채널 결정
            if (!string.IsNullOrWhiteSpace(targetIp) && targetPort > 0)
            {
                // 외부 네트워크 통신용 UDP 채널 설정
                _channel = $"aeron:udp?endpoint={targetIp}:{targetPort}";
            }
            else
            {
                // 단일 서버 내부 통신용 IPC 공유 메모리 채널 설정
                _channel = "aeron:ipc";
            }
        }

        /// <summary>
        /// 미디어 드라이버와 연결합니다.
        /// </summary>
        public void Connect()
        {
            try
            {
                var ctx = new Aeron.Context().AeronDirectoryName(_aeronDir);
                _aeron = Aeron.Connect(ctx);

                // 결정된 _channel 값을 사용하여 구독 추가
                _subscription = _aeron.AddSubscription(_channel, _streamId);

                Logger.LogInformation($"[.NET Subscriber] Aeron 연결 완료. 채널: {_channel}, 스트림 ID: {_streamId} 수신 준비 완료.");
            }
            catch (Exception ex)
            {
                Logger.LogWarning($"[.NET Subscriber] 연결 실패: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// 데이터 수신 루프를 시작합니다.
        /// </summary>
        /// <exception cref="InvalidOperationException"></exception>
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

            Logger.LogInformation("[.NET Subscriber] 데이터 수신 루프를 시작합니다...");

            // 수신 무한 루프
            while (_isRunning)
            {
                int pollCount = 1024;
                int fragmentsRead = _subscription.Poll(assembler.OnFragment, pollCount);
                idleStrategy.Idle(fragmentsRead);
            }
        }

        /// <summary>
        /// 수신 루프를 안전하게 종료합니다.
        /// </summary>
        public void Stop()
        {
            _isRunning = false;
            Logger.LogInformation("[.NET Subscriber] 수신 루프 종료 요청됨.");
        }

        /// <summary>
        /// 수신된 데이터 조각을 처리합니다.
        /// </summary>
        private void OnFragmentReceived(Adaptive.Agrona.IDirectBuffer buffer, int offset, int length, Header aeronHeader)
        {
            // 💡 [참고] 현재 방식은 매 수신마다 byte[] payload = new byte[length] 할당이 발생합니다.
            // C#에서도 Java에서 논의했던 완벽한 Zero-Allocation을 달성하려면, 
            // byte[]를 새로 만들지 않고 Agrona의 버퍼 포인터를 SBE 버퍼에 직접 매핑하는 방법을 고려해 볼 수 있습니다.
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
                OnSingleDataReceived?.Invoke(singleMsg);
            }
            else if (templateId == ListDataMessage.TemplateId)
            {
                var listMsg = new ListDataMessage();
                listMsg.WrapForDecode(sbeBuffer, messageOffset, blockLength, version);
                OnListDataReceived?.Invoke(listMsg);
            }
        }

        /// <summary>
        /// 자원을 해제합니다.
        /// </summary>
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

            Logger.LogInformation("[.NET Subscriber] 자원 해제 완료.");
        }
    }
}