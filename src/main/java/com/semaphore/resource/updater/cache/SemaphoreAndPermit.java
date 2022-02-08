package com.semaphore.resource.updater.cache;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *
 * @date 2021/10/14 2:13 下午
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SemaphoreAndPermit {
    private int permit;
    private MySemaphore rSemaphore;
}
