package com.hooniegit.AeronSubscriber;

import com.hooniegit.sbe.ListDataMessageDecoder;
import com.hooniegit.sbe.SingleDataMessageDecoder;

public interface DataMessageListener {
    // 디코더 객체(포인터)를 직접 전달
    void onSingleDataReceived(SingleDataMessageDecoder decoder);
    void onListDataReceived(ListDataMessageDecoder decoder);
}