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
public class QueryResourceParam {
    /**
     * 需要的数量，用于自动调节一致性，如果缓存数量小于要求数量或者缓存数量为0都有概率进行自调节，可以传空，将不做任何操作
     */
    private Integer acquire;
    private String resourceId;

    @Override
    public boolean equals(Object obj) {
        if(Objects.isNull(obj)){
            return false;
        }
        if(!(obj instanceof QueryResourceParam)){
            return false;
        }
        return ((QueryResourceParam)obj).getResourceId().equals(this.getResourceId());
    }
}
