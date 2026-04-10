package com.hooniegit.AeronJavaTools.Subscriber;

import com.hooniegit.sbe.ListDataMessageDecoder;
import com.hooniegit.sbe.SingleDataMessageDecoder;

/**
 * Subscriber에서 수신된 메시지를 처리하기 위한 리스너 인터페이스입니다.
 * @onSingleDataReceived() 단일 데이터 메시지 수신 시 호출됩니다.
 * @onListDataReceived()는 리스트 데이터 메시지 수신 시 호출됩니다.
 */
public interface DataMessageListener {
    // 디코더 객체(포인터)를 직접 전달
    void onSingleDataReceived(SingleDataMessageDecoder decoder);
    void onListDataReceived(ListDataMessageDecoder decoder);
}