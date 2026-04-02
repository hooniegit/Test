using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Hooniegit.Sbe;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;

namespace AeronCSharpSubscriber
{
    internal class Program
    {
        static void Main(string[] args)
        {

            string aeronDir = System.IO.Path.GetTempPath() + "aeron-sbe-ipc";
            var ctx = new Aeron.Context().AeronDirectoryName(aeronDir);

            using (var aeron = Aeron.Connect(ctx))
            using (var subscription = aeron.AddSubscription("aeron:ipc", 10))
            {
                var sbeBuffer = new Org.SbeTool.Sbe.Dll.DirectBuffer(new byte[0]);

                // 1. 기존의 FragmentHandler 로직을 일반 변수로 분리합니다.
                // 이 핸들러는 조각난 데이터가 모두 합쳐진 '완전한 상태'일 때만 호출됩니다.
                var myDataHandler = new FragmentHandler((buffer, offset, length, aeronHeader) =>
                {
                    // 1. [핵심 수정] 엉뚱한 메모리를 읽지 않도록 정확한 길이만큼 배열에 복사합니다.
                    // 수십 바이트 수준의 복사이므로 성능 저하는 마이크로초(µs) 미만입니다.
                    byte[] payload = new byte[length];
                    buffer.GetBytes(offset, payload, 0, length);

                    var sbeBuffer = new Org.SbeTool.Sbe.Dll.DirectBuffer(payload);

                    // 2. [핵심 수정] 새 배열을 만들었으므로 오프셋 파라미터는 무조건 '0'이 되어야 합니다!
                    var msgHeader = new MessageHeader();
                    msgHeader.Wrap(sbeBuffer, 0, 0);

                    int templateId = msgHeader.TemplateId;
                    int blockLength = msgHeader.BlockLength;
                    int version = msgHeader.Version;

                    // 헤더 이후의 데이터 시작점도 0 + Header.Size 로 계산됩니다.
                    int messageOffset = MessageHeader.Size;

                    if (templateId == SingleDataMessage.TemplateId)
                    {
                        var singleMsg = new SingleDataMessage();
                        singleMsg.WrapForDecode(sbeBuffer, messageOffset, blockLength, version);

                        Console.WriteLine("=== [수신: SingleDataMessage] ===");
                        Console.WriteLine($"ID: {singleMsg.Id}, Value: {singleMsg.Value}");
                        Console.WriteLine($"Timestamp: {singleMsg.GetTimestamp()}");
                    }
                    else if (templateId == ListDataMessage.TemplateId)
                    {
                        var listMsg = new ListDataMessage();
                        listMsg.WrapForDecode(sbeBuffer, messageOffset, blockLength, version);

                        Console.WriteLine("=== [수신: ListDataMessage] ===");

                        // [핵심 해결책] 주석을 반드시 해제해야 합니다!
                        // 데이터를 화면에 출력하지 않더라도, 포인터를 넘기기 위해 Next()는 무조건 호출해야 합니다.
                        var entries = listMsg.Entries;
                        int entryIndex = 1;
                        while (entries.HasNext)
                        {
                            entries.Next(); // 이 메서드가 호출될 때마다 내부 포인터가 다음 항목으로 전진합니다.
                            //Console.WriteLine($"  - Entry {entryIndex}: ID={entries.Id}, Value={entries.Value}");
                            entryIndex++;
                        }

                        // 그룹 순회가 완전히 끝난(HasNext가 false인) 정확한 시점에 문자열을 읽습니다.
                        Console.WriteLine($"Timestamp: {listMsg.GetTimestamp()}");
                    }
                });

                // 2. [핵심] 기존 핸들러를 FragmentAssembler로 감싸줍니다(Wrapping).
                // Assembler가 내부적으로 조각난 메시지를 모아두었다가, 완성이 되면 myDataHandler를 호출해 줍니다.
                var assembler = new FragmentAssembler(myDataHandler);

                var idleStrategy = new SleepingIdleStrategy(1);
                while (true)
                {
                    // 3. Poll 메서드에는 직접 만든 핸들러가 아니라, assembler의 핸들러를 넘겨줍니다.
                    int fragmentsRead = subscription.Poll(assembler.OnFragment, 10);
                    idleStrategy.Idle(fragmentsRead);
                }
            }

        }
    }
}
