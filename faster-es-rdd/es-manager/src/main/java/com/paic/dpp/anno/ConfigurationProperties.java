package com.paic.dpp.anno;

import java.lang.annotation.*;

/**
 * @author dengyu
 * @Function:
 * @date 2020/06/02
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface ConfigurationProperties {
    String prefix() default "";
}
