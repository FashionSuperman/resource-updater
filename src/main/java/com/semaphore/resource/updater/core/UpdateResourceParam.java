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
public class UpdateResourceParam {
    private int num;
    private String resourceId;

    @Override
    public boolean equals(Object obj) {
        if(Objects.isNull(obj)){
            return false;
        }
        if(!(obj instanceof UpdateResourceParam)){
            return false;
        }
        return ((UpdateResourceParam)obj).getResourceId().equals(this.getResourceId());
    }
}
