using System;
using Com.Hooniegit.Sbe;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using Org.SbeTool.Sbe.Dll;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading;
using Adaptive.Agrona;

namespace Tools
{
    /// <summary>
    /// Tag 데이터의 기본 구조체입니다.
    /// </summary>
    /// <typeparam name="T">Value 데이터 타입</typeparam>
    public class TagData<T>
    {
        public int Id { get; set; }
        public T Value { get; set; }
        public TagData() { }
        public TagData(int id, T value) { Id = id; Value = value; }
    }

    /// <summary>
    /// Aeron 미디어 드라이버와 연결하여 SBE(Simple Binary Encoding)로 인코딩된 메시지를 IPC/UDP 채널을 통해 전송합니다.
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
            // Data Storage
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

        /// <summary>
        /// 클라이언트를 미디어 드라이버에 연결합니다.
        /// </summary>
        public void Connect()
        {
            try
            {
                // 기존 자원 정리
                _publication?.Dispose();
                _aeron?.Dispose();

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

        /// <summary>
        /// SingleData 규격의 데이터를 미디어 드라이버에 입력(발행)합니다.
        /// </summary>
        /// <param name="id">Tag ID</param>
        /// <param name="value">Tag Value</param>
        /// <param name="timestamp">Tag Timestamp</param>
        /// <exception cref="InvalidOperationException"></exception>
        public void PublishSingleDataMessage(int id, string value, string timestamp)
        {
            if (!_isConnected) throw new InvalidOperationException("Aeron is not connected.");

            // 현재 스레드의 전용 자원 호출
            var resources = _tlResources.Value; 

            // 버퍼 호출: 동일 메모리에서 분리
            var sbeBuffer = resources.SbeBuffer;
            var aeronBuffer = resources.AeronBuffer;

            // SBE Message Header 인코딩
            resources.HeaderEncoder.Wrap(sbeBuffer, 0, SingleDataMessage.SchemaVersion);
            resources.HeaderEncoder.BlockLength = SingleDataMessage.BlockLength;
            resources.HeaderEncoder.TemplateId = SingleDataMessage.TemplateId;
            resources.HeaderEncoder.SchemaId = SingleDataMessage.SchemaId;
            resources.HeaderEncoder.Version = SingleDataMessage.SchemaVersion;

            // SBE Message SingleData 인코딩
            resources.SingleEncoder.WrapForEncode(sbeBuffer, MessageHeader.Size);
            resources.SingleEncoder.Id = id;
            resources.SingleEncoder.SetValue(value);
            resources.SingleEncoder.SetTimestamp(timestamp);

            // 전체 메시지 길이 계산
            int msgLength = MessageHeader.Size + resources.SingleEncoder.Size;

            // 데이터 입력(발행)
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

        /// <summary>
        /// ListData 규격의 데이터를 미디어 드라이버에 입력(발행)합니다.
        /// </summary>
        /// <param name="dataList">TagData List</param>
        /// <param name="timestamp">Tag Timestamp</param>
        /// <exception cref="InvalidOperationException"></exception>
        public void PublishListDataMessage(List<TagData<double>> dataList, string timestamp)
        {
            if (!_isConnected) throw new InvalidOperationException("[.NET Publisher] 미디어 드라이버와 연결되지 않았습니다. Connect() 메서드를 통해 연결하십시오.");

            // 현재 스레드의 전용 자원 호출
            var resources = _tlResources.Value;

            // 버퍼 호출: 동일 메모리에서 분리
            var sbeBuffer = resources.SbeBuffer;
            var aeronBuffer = resources.AeronBuffer;

            // SBE Message Header 인코딩
            resources.HeaderEncoder.Wrap(sbeBuffer, 0, ListDataMessage.SchemaVersion);
            resources.HeaderEncoder.BlockLength = ListDataMessage.BlockLength;
            resources.HeaderEncoder.TemplateId = ListDataMessage.TemplateId;
            resources.HeaderEncoder.SchemaId = ListDataMessage.SchemaId;
            resources.HeaderEncoder.Version = ListDataMessage.SchemaVersion;

            // SBE Message ListData 인코딩
            resources.ListEncoder.WrapForEncode(sbeBuffer, MessageHeader.Size);
            var entries = resources.ListEncoder.EntriesCount(dataList.Count);
            for (int i = 0; i < dataList.Count; i++) // foreach 대신 for 사용: 이터레이터 객체 생성 방지
            {
                entries.Next();
                entries.Id = dataList[i].Id;
                entries.Value = dataList[i].Value;
            }
            resources.ListEncoder.SetTimestamp(timestamp);

            // 전체 메세지 길이 계싼
            int msgLength = MessageHeader.Size + resources.ListEncoder.Size;

            // 메세지 입력(발행)
            long result;
            while ((result = _publication.Offer(aeronBuffer, 0, msgLength)) < 0L)
            {
                if (result == Publication.BACK_PRESSURED) Thread.Yield();
                else if (result == Publication.NOT_CONNECTED) break;
            }
        }

        /// <summary>
        /// 연결 및 자원 메모리를 해제합니다.
        /// </summary>
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
    /// Aeron 미디어 드라이버와 연결하여 SBE(Simple Binary Encoding)로 인코딩된 메세지를 IPC/UDP 채널을 통해 수신합니다.
    /// </summary>
    public class DataSubscriber : IDisposable, IErrorHandler
    {
        // Aeron
        private readonly string _aeronDir;
        private Aeron _aeron;
        private Subscription _subscription;
        private readonly int _streamId;
        private readonly string _channel;
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

        // SBE Payload
        private readonly byte[] _cachedPayload;
        private readonly DirectBuffer _sbeBuffer;
        private readonly MessageHeader _msgHeader = new MessageHeader();
        private readonly SingleDataMessage _singleMsg = new SingleDataMessage();
        private readonly ListDataMessage _listMsg = new ListDataMessage();

        //
        private long _lastReceivedTicks;
        private readonly TimeSpan _timeoutThreshold = TimeSpan.FromSeconds(3); // 3초 타임아웃
        private bool _isCurrentlyConnected = false;

        // 외부에서 연결/끊김 상태를 알 수 있도록 Action 추가
        public Action OnConnected { get; set; }
        public Action OnDisconnected { get; set; }

        public DataSubscriber(string aeronDir = null, int streamId = 0, string targetIp = null, int targetPort = -1, byte[] sharedBuffer = null)
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

            // 외부 Buffer 참조, 없을 경우 신규 생성
            _cachedPayload = sharedBuffer ?? new byte[1024 * 1024];
            _sbeBuffer = new DirectBuffer(_cachedPayload);
        }

        /// <summary>
        /// 미디어 드라이버와 연결합니다.
        /// </summary>
        public void Connect()
        {
            try
            {
                // Context에 Image 핸들러(콜백) 등록
                var ctx = new Aeron.Context()
                    .AeronDirectoryName(_aeronDir)
                    .AvailableImageHandler(OnAvailableImage)
                    .UnavailableImageHandler(OnUnavailableImage)
                    .ErrorHandler(this);

                _aeron = Aeron.Connect(ctx);
                _subscription = _aeron.AddSubscription(_channel, _streamId);

                Logger.LogInformation($"[.NET Subscriber] Aeron 준비 완료. 채널: {_channel}, 스트림 ID: {_streamId}");
            }
            catch (Exception ex)
            {
                Logger.LogWarning($"[.NET Subscriber] 연결 실패: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// 발행자(Publisher)가 감지되어 데이터 수신이 가능한 상태가 될 때 호출됩니다.
        /// </summary>
        private void OnAvailableImage(Image image)
        {
            Logger.LogInformation($"[.NET Subscriber] 연결 감지됨 (SessionId: {image.SessionId})");
            OnConnected?.Invoke();
        }

        /// <summary>
        /// 발행자(Publisher)가 종료되거나 타임아웃되어 연결이 끊어졌을 때 호출됩니다.
        /// </summary>
        private void OnUnavailableImage(Image image)
        {
            Logger.LogWarning($"[.NET Subscriber] 연결 끊어짐 감지 (SessionId: {image.SessionId})");
            OnDisconnected?.Invoke();
        }

        /// <summary>
        /// Aeron 내부 백그라운드 스레드에서 발생하는 에러를 잡아 앱 크래시를 방지합니다.
        /// </summary>
        public void OnError (Exception ex)
        {
            Logger.LogError($"[.NET Subscriber] Aeron 백그라운드 에러 감지: {ex.Message}");

            // 에러가 났다는 것은 드라이버가 죽었거나 심각한 통신 장애가 생겼다는 뜻이므로,
            // 수신 루프(_isRunning)를 강제로 멈추고 외부 루프에 연결이 끊겼음을 알립니다.
            Stop();
            OnDisconnected?.Invoke();
        }

        /// <summary>
        /// 데이터 수신 루프를 시작합니다.
        /// </summary>
        /// <exception cref="InvalidOperationException"></exception>
        public void Start()
        {
            if (_subscription == null)
            {
                throw new InvalidOperationException("[.NET Subscriber] 미디어 드라이버와 연결되지 않았습니다. Connect() 메서드를 통해 연결하십시오.");
            }

            _isRunning = true;

            // 핸들러를 FragmentAssembler로 래핑
            var myDataHandler = new FragmentHandler(OnFragmentReceived);
            var assembler = new FragmentAssembler(myDataHandler);

            var idleStrategy = new SleepingIdleStrategy(1); // 평시용
            // var idleStrategy = new BusySpinIdleStrategy(); // 초고성능용

            Logger.LogInformation("[.NET Subscriber] 데이터 수신 루프를 시작합니다...");

            _isRunning = true;
            _lastReceivedTicks = DateTime.UtcNow.Ticks; // 시작 시간 기록

            // 수신 무한 루프
            while (_isRunning)
            {
                int fragmentsRead = _subscription.Poll(assembler.OnFragment, 1024);
                if (fragmentsRead > 0)
                {
                    // 데이터 수신: 시간 갱신 및 연결 상태 복구
                    Interlocked.Exchange(ref _lastReceivedTicks, DateTime.UtcNow.Ticks);

                    if (!_isCurrentlyConnected)
                    {
                        _isCurrentlyConnected = true;
                        Logger.LogInformation("[.NET Subscriber] 데이터 수신 재개 - 연결 복구됨");
                    }
                }

                // 타임아웃 체크 로직
                // (너무 잦은 연산을 막기 위해 fragmentsRead가 0일 때만 체크하거나, 주기를 둘 수 있습니다)
                if (_isCurrentlyConnected &&
                    TimeSpan.FromTicks(DateTime.UtcNow.Ticks - Interlocked.Read(ref _lastReceivedTicks)) > _timeoutThreshold)
                {
                    _isCurrentlyConnected = false;
                    Logger.LogWarning("[.NET Subscriber] TimeOut - 발행자와의 연결이 끊겼거나 데이터가 오지 않습니다.");
                    OnDisconnected?.Invoke();
                }

                idleStrategy.Idle(fragmentsRead);
            }
        }

        /// <summary>
        /// 수신 루프를 종료합니다.
        /// </summary>
        public void Stop()
        {
            _isRunning = false;
            Logger.LogInformation("[.NET Publisher] 데이터 수신 루프가 종료되었습니다.");
        }

        /// <summary>
        /// 수신된 데이터 조각을 처리합니다.
        /// </summary>
        private void OnFragmentReceived(Adaptive.Agrona.IDirectBuffer buffer, int offset, int length, Header aeronHeader)
        {
            // Zero-Allocation (_cachedPayload 재활용)
            buffer.GetBytes(offset, _cachedPayload, 0, length);
            // 주의: 복사된 데이터는 배열의 0번 인덱스부터 시작하므로 offset을 0으로 줍니다.
            _msgHeader.Wrap(_sbeBuffer, 0, 0);

            int templateId = _msgHeader.TemplateId;
            int blockLength = _msgHeader.BlockLength;
            int version = _msgHeader.Version;
            int messageOffset = MessageHeader.Size;

            // 메시지 템플릿별 분기 처리 (새로 할당하지 않고 재사용)
            if (templateId == SingleDataMessage.TemplateId)
            {
                _singleMsg.WrapForDecode(_sbeBuffer, messageOffset, blockLength, version);
                OnSingleDataReceived?.Invoke(_singleMsg);
            }
            else if (templateId == ListDataMessage.TemplateId)
            {
                _listMsg.WrapForDecode(_sbeBuffer, messageOffset, blockLength, version);
                OnListDataReceived?.Invoke(_listMsg);
            }
        }

        /// <summary>
        /// 연결 및 자원 메모리를 해제합니다.
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