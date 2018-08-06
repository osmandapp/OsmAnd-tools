package net.osmand.server;

import net.osmand.server.index.IndexResourceBalancingFilter;
import net.osmand.server.index.IndexResourceResolver;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@EnableWebMvc
public class WebConfiguration implements WebMvcConfigurer {

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/download.php*")
                .addResourceLocations("file:/var/www-download/")
                .resourceChain(false)
                .addResolver(new IndexResourceResolver());
    }

    @Bean
    FilterRegistrationBean<IndexResourceBalancingFilter> registrationBean(IndexResourceBalancingFilter filter) {
        FilterRegistrationBean<IndexResourceBalancingFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(filter);
        registrationBean.addUrlPatterns("/download.php");
        return registrationBean;
    }

    @Bean
    public IndexResourceBalancingFilter myFilter() {
        return new IndexResourceBalancingFilter();
    }
}
