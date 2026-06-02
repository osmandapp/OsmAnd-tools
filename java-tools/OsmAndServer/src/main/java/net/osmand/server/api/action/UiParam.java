package net.osmand.server.api.action;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface UiParam {
    String name() default "";
    String title() default "";
    boolean required() default false;
    String defaultValue() default "";
}
