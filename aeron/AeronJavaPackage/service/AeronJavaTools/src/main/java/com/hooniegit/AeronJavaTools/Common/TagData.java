package com.hooniegit.AeronJavaTools.Common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Tag 데이터의 표준 규격 클래스입니다.
 * @param <T>
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TagData<T> {
    private int id;
    private T value;
}
