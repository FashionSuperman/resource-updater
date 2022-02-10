package com.semaphore.resource.updater.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 *
 * @date 2021/10/13 9:42 上午
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryResourceResult {
    private String resourceId;
    /**
     * 要求的数量
     */
    private int acquire;
    /**
     * 实际数量（缓存）
     */
    private int num;
    /**
     * 是否满足acquire的要求
     */
    private boolean fill;

    @Override
    public boolean equals(Object obj) {
        if(Objects.isNull(obj)){
            return false;
        }
        if(!(obj instanceof QueryResourceResult)){
            return false;
        }
        return ((QueryResourceResult)obj).getResourceId().equals(this.getResourceId());
    }
}
