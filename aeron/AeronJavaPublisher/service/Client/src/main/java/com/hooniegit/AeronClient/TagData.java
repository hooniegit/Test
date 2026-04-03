package com.hooniegit.AeronClient;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 테스트용 샘플 데이터 객체입니다.
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
