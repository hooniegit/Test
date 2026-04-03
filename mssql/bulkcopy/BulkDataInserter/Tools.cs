using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;

namespace Tools
{
    /// <summary>
    /// Bulk Insert 용 데이터 객체
    /// </summary>
    public class DataState
    {
        public int id;
        public string value;
        public string timestamp;

        // 데이터 상태 관리
        // 0 = Clean
        // 1 = Dirty
        // 2 = Flushing
        public int Status = 0;

        public void UpdateIfChanged(string newTimestamp, string newValue)
        {
            if (this.value != newValue)
            {
                // 스레드 안전성 확보 (단일 객체 단위의 lock: 경합 확률 0%)
                lock (this)
                {
                    if (this.value != newValue)
                    {
                        this.value = newValue;
                        this.timestamp = newTimestamp;
                        this.Status = 1;
                    }
                }
            }
        }
    }

    /// <summary>
    /// Custom IDataReader 
    /// </summary>
    public class StateDataReader : IDataReader
    {
        private readonly List<DataState> _records;
        private int _currentIndex = -1;

        public StateDataReader(List<DataState> records)
        {
            _records = records;
        }

        // =========================================================================
        // SqlBulkCopy 구동을 위해 반드시 필요한 핵심 구현
        // =========================================================================

        public int FieldCount => 3; // DataState Property Count (Id, Value, Timestamp)

        public bool Read()
        {
            _currentIndex++;
            return _currentIndex < _records.Count;
        }

        public object GetValue(int i)
        {
            switch (i)
            {
                case 0:
                    return _records[_currentIndex].id;
                case 1:
                    return _records[_currentIndex].value;
                case 2:
                    return _records[_currentIndex].timestamp;
                default:
                    throw new IndexOutOfRangeException();
            }
        }

        // SqlBulkCopy가 컬럼 매핑 시 null 여부를 확인할 수 있으므로, 
        // 예외를 던지면 에러가 발생할 수 있어 false를 반환하도록 안전하게 처리합니다.
        public bool IsDBNull(int i) => false;

        // using 블록이나 SqlBulkCopy 내부에서 객체를 닫을 때 호출되므로,
        // 예외를 던지지 않고 비워둡니다.
        public void Dispose() { }
        public void Close() { }

        // 정상적인 상태임을 알리기 위해 false 반환
        public bool IsClosed => false;

        // =========================================================================
        // 선택 메서드 및 속성 (NotSupportedException 처리)
        // : 만약 SqlBulkCopy에서 ColumnMappings를 이름으로 매핑한다면 이 두 메서드는 구현이 필요할 수 있습니다.
        // =========================================================================

        public string GetName(int i) => throw new NotSupportedException();
        public int GetOrdinal(string name) => throw new NotSupportedException();

        // =========================================================================
        // 미사용 메서드 및 속성 (NotSupportedException 처리)
        // =========================================================================

        public object this[int i] => throw new NotSupportedException();
        public object this[string name] => throw new NotSupportedException();
        public int Depth => throw new NotSupportedException();
        public int RecordsAffected => throw new NotSupportedException();
        public DataTable GetSchemaTable() => throw new NotSupportedException();
        public bool NextResult() => throw new NotSupportedException();
        public bool GetBoolean(int i) => throw new NotSupportedException();
        public byte GetByte(int i) => throw new NotSupportedException();
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotSupportedException();
        public char GetChar(int i) => throw new NotSupportedException();
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotSupportedException();
        public IDataReader GetData(int i) => throw new NotSupportedException();
        public string GetDataTypeName(int i) => throw new NotSupportedException();
        public DateTime GetDateTime(int i) => throw new NotSupportedException();
        public decimal GetDecimal(int i) => throw new NotSupportedException();
        public double GetDouble(int i) => throw new NotSupportedException();
        public Type GetFieldType(int i) => throw new NotSupportedException();
        public float GetFloat(int i) => throw new NotSupportedException();
        public Guid GetGuid(int i) => throw new NotSupportedException();
        public short GetInt16(int i) => throw new NotSupportedException();
        public int GetInt32(int i) => throw new NotSupportedException();
        public long GetInt64(int i) => throw new NotSupportedException();
        public string GetString(int i) => throw new NotSupportedException();
        public int GetValues(object[] values) => throw new NotSupportedException();
    }

    /// <summary>
    /// 
    /// </summary>
    public class StreamProcessor
    {
        // ID가 제한적이고 연속적이라면 DataState[] 배열을 쓰는 것이 조회 비용이 0이므로 가장 좋습니다.
        private readonly ConcurrentDictionary<int, DataState> _stateMap = new ConcurrentDictionary<int, DataState>();

        // 매번 List를 생성하지 않기 위한 재사용 버퍼 (Zero-Allocation)
        private readonly List<DataState> _flushBuffer;
        private readonly string _connectionString;
        private readonly int _batchSize;

        // @AllArgsConstructor
        public StreamProcessor(string connectionString, int bufferSize, int batchSize = 5000) {
            _connectionString = connectionString;
            _flushBuffer = new List<DataState>(batchSize);
            _batchSize = batchSize;
        }

        // [Real-Time] 데이터 수신부
        public void OnDataReceived(int id, string timestamp, string value)
        {
            var state = _stateMap.GetOrAdd(id, key => new DataState { id = key });
            state.UpdateIfChanged(timestamp, value);
        }

        // [Schedule] DB Insert Batch 작업 
        public void FlushToDatabase(string TargetTable)
        {
            _flushBuffer.Clear();

            // Dirty 레코드 수집
            foreach (var state in _stateMap.Values)
            {
                if (state.Status == 1) // Dirty인 경우
                {
                    lock (state)
                    {
                        // 상태가 여전히 1이라면 Flushing(2)으로 변경하고 버퍼에 추가
                        if (state.Status == 1)
                        {
                            state.Status = 2;
                            _flushBuffer.Add(state);
                        }
                    }
                }
            }

            if (_flushBuffer.Count == 0) return;

            // SqlBulkCopy로 일괄 Insert (Zero-Copy)
            using (var bulkCopy = new SqlBulkCopy(_connectionString, SqlBulkCopyOptions.TableLock))
            {
                bulkCopy.DestinationTableName = TargetTable;
                bulkCopy.BatchSize = _batchSize;

                // _flushBuffer 데이터 INSERT
                using (var reader = new StateDataReader(_flushBuffer))
                {
                    bulkCopy.WriteToServer(reader);
                }
            }

            // INSERT 완료 후 DataState 상태 초기화: 전송 중 갱신된 데이터 누락 방지
            foreach (var state in _flushBuffer)
            {
                lock (state)
                {
                    // Status가 2(Flushing)라면 그동안 새 데이터가 안 들어온 것이므로 0(Clean)으로 초기화
                    // OnDataReceived가 호출되었다면 Status는 1이 되어 있을 것이므로 무시됨 (다음 주기에 다시 전송)
                    if (state.Status == 2)
                    {
                        state.Status = 0;
                    }
                }
            }

            // TEST
            Console.WriteLine("[2] 데이터 INSERT 완료");
        }
    }
}

