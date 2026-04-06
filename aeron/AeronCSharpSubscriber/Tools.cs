using System;
using Com.Hooniegit.Sbe;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using Org.SbeTool.Sbe.Dll;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading;

namespace Tools
{
    /// <summary>
    /// Sample 데이터 규격
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class TagData<T>
    {
        public int Id { get; set; }
        public T Value { get; set; }
        public TagData(int id, T value) { Id = id; Value = value; }
    }

    /// <summary>
    /// Aeron 미디어 드라이버와 연결하여 SBE(Simple Binary Encoding)로 인코딩된 메시지를 IPC 채널을 통해 전송합니다.
    /// </summary>
    public class DataPublisher : IDisposable
    {
        // Aeron
        private readonly string _aeronDir;
        private Aeron _aeron;
        private Publication _publication;
        private readonly int _streamId;
        private readonly string _channel;
        private volatile bool _isConnected = false;

        // Logging
        private static readonly ILoggerFactory LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
            builder.SetMinimumLevel(LogLevel.Debug);
        });
        private static readonly ILogger Logger = LoggerFactory.CreateLogger<DataPublisher>();


        /// <summary>
        /// Multi-Thread 환경에서 독립적으로 인코딩 자원을 관리하는 컨테이너입니다.
        /// </summary>
        private class EncodingResources
        {
            // 1. 실제 데이터가 담길 물리적인 메모리 공간
            public readonly byte[] UnderlyingArray;

            // SBE Encoding
            public readonly DirectBuffer SbeBuffer;
            public readonly MessageHeader HeaderEncoder;
            public readonly SingleDataMessage SingleEncoder;
            public readonly ListDataMessage ListEncoder;

            // Aeron
            public readonly UnsafeBuffer AeronBuffer;

            public EncodingResources()
            {
                UnderlyingArray = new byte[1024 * 1024]; // 1MB

                // Zero-Copy: 두 버퍼 모두 동일한 UnderlyingArray 참조
                SbeBuffer = new DirectBuffer(UnderlyingArray);
                AeronBuffer = new UnsafeBuffer(UnderlyingArray);

                HeaderEncoder = new MessageHeader();
                SingleEncoder = new SingleDataMessage();
                ListEncoder = new ListDataMessage();
            }
        }

        // C# ThreadLocal: 
        private readonly ThreadLocal<EncodingResources> _tlResources =
            new ThreadLocal<EncodingResources>(() => new EncodingResources());

        public DataPublisher(string aeronDir = null, int streamId = 0, string targetIp = null, int targetPort = -1)
        {
            _aeronDir = aeronDir ?? System.IO.Path.GetTempPath() + "aeron-sbe-ipc";
            _streamId = streamId;

            // IP/Port 유무에 따라 IPC 또는 UDP 채널 동적 할당
            if (!string.IsNullOrWhiteSpace(targetIp) && targetPort > 0)
            {
                _channel = $"aeron:udp?endpoint={targetIp}:{targetPort}";
            }
            else
            {
                _channel = "aeron:ipc";
            }
        }

        public void Connect()
        {
            try
            {
                var ctx = new Aeron.Context().AeronDirectoryName(_aeronDir);
                _aeron = Aeron.Connect(ctx);
                _publication = _aeron.AddPublication(_channel, _streamId);
                _isConnected = true;

                Logger.LogInformation($"[.NET Publisher] Aeron 연결 완료. 채널: {_channel}, 스트림 ID: {_streamId}");
            }
            catch (Exception ex)
            {
                Logger.LogWarning($"[.NET Publisher] 연결 실패: {ex.Message}");
                _isConnected = false;
                throw;
            }
        }

        public void PublishSingleDataMessage(int id, string value, string timestamp)
        {
            if (!_isConnected) throw new InvalidOperationException("Aeron is not connected.");

            // 현재 스레드의 전용 자원을 가져옴
            var resources = _tlResources.Value;

            // 💡 용도에 맞게 버퍼를 분리해서 꺼냅니다. (둘은 같은 메모리를 공유합니다)
            var sbeBuffer = resources.SbeBuffer;
            var aeronBuffer = resources.AeronBuffer;

            // 1. SBE 메시지 헤더 인코딩 (SbeBuffer 사용)
            resources.HeaderEncoder.Wrap(sbeBuffer, 0, SingleDataMessage.SchemaVersion);
            resources.HeaderEncoder.BlockLength = SingleDataMessage.BlockLength;
            resources.HeaderEncoder.TemplateId = SingleDataMessage.TemplateId;
            resources.HeaderEncoder.SchemaId = SingleDataMessage.SchemaId;
            resources.HeaderEncoder.Version = SingleDataMessage.SchemaVersion;

            // 2. SBE 본문 인코딩 (SbeBuffer 사용)
            resources.SingleEncoder.WrapForEncode(sbeBuffer, MessageHeader.Size);
            resources.SingleEncoder.Id = id;

            // value 인코딩 (string 타입)
            resources.SingleEncoder.SetValue(value);

            // timestamp 인코딩
            resources.SingleEncoder.SetTimestamp(timestamp);

            // 전체 메시지 길이 계산
            int msgLength = MessageHeader.Size + resources.SingleEncoder.Size;

            // 3. Aeron을 통한 전송 (AeronBuffer 사용)
            long result;
            while ((result = _publication.Offer(aeronBuffer, 0, msgLength)) < 0L)
            {
                if (result == Publication.BACK_PRESSURED)
                {
                    Thread.Yield();
                }
                else if (result == Publication.NOT_CONNECTED)
                {
                    break;
                }
            }
        }
        public void PublishListDataMessage(List<TagData<double>> dataList, string timestamp)
        {
            if (!_isConnected) throw new InvalidOperationException("Aeron is not connected.");

            var resources = _tlResources.Value;

            // 💡 용도에 맞게 버퍼를 분리해서 꺼냅니다. (둘은 같은 메모리를 공유합니다)
            var sbeBuffer = resources.SbeBuffer;
            var aeronBuffer = resources.AeronBuffer;


            // 1. SBE 메시지 헤더 인코딩 (SbeBuffer 사용)
            resources.HeaderEncoder.Wrap(sbeBuffer, 0, ListDataMessage.SchemaVersion);
            resources.HeaderEncoder.BlockLength = ListDataMessage.BlockLength;
            resources.HeaderEncoder.TemplateId = ListDataMessage.TemplateId;
            resources.HeaderEncoder.SchemaId = ListDataMessage.SchemaId;
            resources.HeaderEncoder.Version = ListDataMessage.SchemaVersion;

            // 2. 본문 인코딩
            resources.ListEncoder.WrapForEncode(sbeBuffer, MessageHeader.Size);

            // 그룹(List) 데이터 인코딩
            var entries = resources.ListEncoder.EntriesCount(dataList.Count);
            for (int i = 0; i < dataList.Count; i++) // foreach 대신 for를 사용해 이터레이터 객체 생성 방지
            {
                entries.Next();
                entries.Id = dataList[i].Id;
                entries.Value = dataList[i].Value;
            }

            // 문자열 인코딩 (반드시 반복문 종료 후에 처리)
            resources.ListEncoder.SetTimestamp(timestamp);

            int msgLength = MessageHeader.Size + resources.ListEncoder.Size;

            // 3. 데이터 전송
            long result;
            while ((result = _publication.Offer(aeronBuffer, 0, msgLength)) < 0L)
            {
                if (result == Publication.BACK_PRESSURED) Thread.Yield();
                else if (result == Publication.NOT_CONNECTED) break;
            }
        }

        public void Dispose()
        {
            _isConnected = false;

            if (_publication != null) _publication.Dispose();
            if (_aeron != null) _aeron.Dispose();

            // ThreadLocal 자원 메모리 해제
            if (_tlResources != null) _tlResources.Dispose();

            Logger.LogInformation("[.NET Publisher] 자원 해제 완료.");
        }
    }

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