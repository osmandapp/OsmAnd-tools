package net.osmand.server.index;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class IndexResourceBalancingFilter implements Filter {

    private boolean isContainAndEqual(String param, String equalTo, Map<String, String[]> params) {
        return params.containsKey(param) && params.get(param) != null && params.get(param).length > 0
                && params.get(param)[0].equalsIgnoreCase(equalTo);
    }

    private boolean isContainAndEqual(String param, Map<String, String[]> params) {
        return isContainAndEqual(param, "yes", params);
    }

    private boolean computeSimpleCondition(Map<String, String[]> params) {
        return isContainAndEqual("wiki", params)
                || isContainAndEqual("standard", params)
                || isContainAndEqual("road", params)
                || isContainAndEqual("wikivoyage", params);
    }

    private boolean computeStayHereCondition(Map<String, String[]> params) {
        return isContainAndEqual("osmc", params)
                || isContainAndEqual("aosmc", params)
                || isContainAndEqual("fonts", params)
                || isContainAndEqual("inapp", params);
    }

    @Autowired
    private DownloadProperties config;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        /*
            Do nothing
         */
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        DownloadProperties.DownloadServers servers = config.getServers();
        String hostName = request.getRemoteHost();
		if (hostName == null) {
			httpResponse.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid host name");
			return;
		}
		boolean self = isContainAndEqual("self", "true", request.getParameterMap());
		if (self) {
			chain.doFilter(request, response);
			return;
		}
		if (hostName.equals(config.getServers().getSelf())) {
			ThreadLocalRandom tlr = ThreadLocalRandom.current();
			int random = tlr.nextInt(100);
			boolean isSimple = computeSimpleCondition(request.getParameterMap());
			if (computeStayHereCondition(request.getParameterMap())) {
                chain.doFilter(request, response);
			} else if (!servers.getHelp().isEmpty() && isSimple && random < (100 - config.getLoad())) {
				String host = servers.getHelp().get(random % servers.getHelp().size());
				httpResponse.sendRedirect("http://" + host + "/download?" + httpRequest.getQueryString());
			} else if (!servers.getMain().isEmpty()) {
				String host = servers.getMain().get(random % servers.getMain().size());
                httpResponse.sendRedirect("http://" + host + "/download?" + httpRequest.getQueryString());
			} else {
                chain.doFilter(request, response);
			}
			return;
		}
		chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        /*
            Do nothing
         */
    }

    @Configuration
    @ConfigurationProperties(prefix = "download")
    public static class DownloadProperties {
        private int load;
        private DownloadServers servers = new DownloadServers();

        public int getLoad() {
            return load;
        }

        public void setLoad(int load) {
            this.load = load;
        }

        public DownloadServers getServers() {
            return servers;
        }

        public void setServers(DownloadServers servers) {
            this.servers = servers;
        }

        public static class DownloadServers {
            private String self;
            private List<String> help = new ArrayList<>();
            private List<String> main = new ArrayList<>();

            public String getSelf() {
                return self;
            }

            public void setSelf(String self) {
                this.self = self;
            }

            public List<String> getHelp() {
                return help;
            }

            public void setHelp(List<String> help) {
                this.help = help;
            }

            public List<String> getMain() {
                return main;
            }

            public void setMain(List<String> main) {
                this.main = main;
            }
        }
    }
}
