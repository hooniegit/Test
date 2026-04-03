using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Tools;

namespace BulkDataInserter
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // 본인의 MSSQL 접속 정보로 수정하세요.
            string connectionString = "Server=workspace;Database=ToolState;User Id=sa;Password=!@34qwer;TrustServerCertificate=True;";
            var processor = new StreamProcessor(connectionString, 100000, 5000);

            // TEST: DB Insert Batch
            string tableName = "Test";
            int durationSeconds = 3;
            Timer flushTimer = new Timer(state =>
            {
                processor.FlushToDatabase(tableName);
            }, null, TimeSpan.FromSeconds(durationSeconds), TimeSpan.FromSeconds(durationSeconds));

            Console.WriteLine("실시간 데이터 수신 시뮬레이션 시작... (종료하려면 Enter 키를 누르세요)");

            // TEST: Data Generation & Receive
            Task.Run(() =>
            {
                Random rand = new Random();
                while (true)
                {
                    int randomId = rand.Next(1, 101); // 1~100 사이의 ID
                    string newValue = $"Val_{rand.Next(1, 10)}"; // 값이 빈번하게 변경됨을 가정
                    string timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss.s");

                    processor.OnDataReceived(randomId, timestamp, newValue);

                    // TEST
                    Console.WriteLine("[1] 데이터 갱신 완료");

                    Thread.Sleep(1); // 가상의 지연
                }
            });

            Console.ReadLine(); // 메인 스레드 대기
            flushTimer.Dispose();
        }
    }
}
