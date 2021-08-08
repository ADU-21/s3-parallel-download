package com.adu21.s3download;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author LukeDu
 * @date 2021/8/8
 */
@Data
@AllArgsConstructor
public class Chunk {
    private String head;
    private String tail;
    private int index;
}
