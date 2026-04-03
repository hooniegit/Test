package com.hooniegit.AeronClient;

import com.hooniegit.sbe.ListDataMessageEncoder;
import com.hooniegit.sbe.MessageHeaderEncoder;
import com.hooniegit.sbe.SingleDataMessageEncoder;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.List;

public class DataPublisher_b {

    private final String aeronDir = System.getProperty("java.io.tmpdir") + "/aeron-sbe-ipc";
    private final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context().aeronDirectoryName(aeronDir).dirDeleteOnStart(true);

    private Aeron aeron;
    private Publication publication;

    private boolean isConnected = false;

    private UnsafeBuffer buffer;
    private MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private SingleDataMessageEncoder singleEncoder = new SingleDataMessageEncoder();
    private ListDataMessageEncoder listEncoder = new ListDataMessageEncoder();

    public void connect() {
        try{
            this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
            this.publication = aeron.addPublication("aeron:ipc", 10);

            this.buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(33_554_432));
            this.isConnected = true;
        } catch(Exception e){
            e.printStackTrace();
            this.isConnected = false;
        }
    }

    public void publishSingleDataMessage(int id, double value, String timestamp) {
        if (!isConnected) {
            throw new IllegalStateException("Aeron is not connected. Call connect() before publishing.");
        }
        long result;

        singleEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
        singleEncoder.id(id)
                .value(value)
                .timestamp(timestamp);
        int singleMsgLength = MessageHeaderEncoder.ENCODED_LENGTH + singleEncoder.encodedLength();
        while ((result = publication.offer(buffer, 0, singleMsgLength)) < 0L) {
            Thread.yield();
        }

        System.out.println("[Java] SingleDataMessage 전송 완료");
    }

    public void publishListDataMessage(List<TagData<Double>> dataList, String timestamp) {
        if (!isConnected) {
            throw new IllegalStateException("Aeron is not connected. Call connect() before publishing.");
        }
        long result;

        this.listEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);

        ListDataMessageEncoder.EntriesEncoder entries = listEncoder.entriesCount(dataList.size());
        for (TagData<Double> data : dataList) {
            entries.next()
                    .id(data.getId())
                    .value(data.getValue());
        }
        this.listEncoder.timestamp(timestamp);

        int listMsgLength = MessageHeaderEncoder.ENCODED_LENGTH + listEncoder.encodedLength();

        while ((result = publication.offer(buffer, 0, listMsgLength)) < 0L) {
            Thread.yield();
        }

        System.out.println("[Java] ListDataMessage 전송 완료 : " + timestamp);
    }

    public void disconnect() {
        if (publication != null) {
            publication.close();
        }
        if (aeron != null) {
            aeron.close();
        }
        this.isConnected = false;
    }

}
