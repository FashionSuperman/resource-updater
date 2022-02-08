package com.semaphore.resource.updater.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *
 * @date 2021/10/12 2:55 下午
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ResourcePermit {
    private String resourceId;
    private int availableCount;
    private int preLockCount;
}
