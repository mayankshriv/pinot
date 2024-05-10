import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.spi.utils.CommonConstants;
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

public class ControllerAdminApiApplication extends ResourceConfig {
    private final String _controllerResourcePackages;
    private final boolean _useHttps;
    private HttpServer _httpServer;

    public ControllerAdminApiApplication(ControllerConf conf) {
        super();
        property("pinotConfiguration", conf);

        _controllerResourcePackages = conf.getControllerResourcePackages();
        packages(_controllerResourcePackages);
        _useHttps = Boolean.parseBoolean(conf.getProperty(ControllerConf.CONSOLE_SWAGGER_USE_HTTPS));
        if (conf.getProperty(CommonConstants.Controller.CONTROLLER_SERVICE_AUTO_DISCOVERY, false)) {
            register(ServiceAutoDiscoveryFeature.class);
        }
        register(JacksonFeature.class);
        register(MultiPartFeature.class);
        registerClasses(io.swagger.jaxrs.listing.ApiListingResource.class, io.swagger.jaxrs.listing.SwaggerSerializers.class);
        register(new CorsFilter());
        register(AuthenticationFilter.class);
    }

    public void registerBinder(AbstractBinder binder) {
        register(binder);
    }

    public void start(List<ListenerConfig> listenerConfigs) {
        _httpServer = ListenerConfigUtil.buildHttpServer(this, listenerConfigs);

        try {
            _httpServer.start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start http server", e);
        }
        setupSwagger();

        ClassLoader classLoader = ControllerAdminApiApplication.class.getClassLoader();
        _httpServer.getServerConfiguration().addHttpHandler(
            new SafeStaticHttpHandler(classLoader, "/webapp/"), "/index.html");
        _httpServer.getServerConfiguration().addHttpHandler(
            new SafeStaticHttpHandler(classLoader, "/webapp/images/"), "/images/");
        _httpServer.getServerConfiguration().addHttpHandler(
            new SafeStaticHttpHandler(classLoader, "/webapp/js/"), "/js/");
    }

    private void setupSwagger() {
        BeanConfig beanConfig = new BeanConfig();
        beanConfig.setTitle("Pinot Controller API");
        beanConfig.setDescription("APIs for accessing Pinot Controller information");
        beanConfig.setContact("https://github.com/apache/pinot");
        beanConfig.setVersion("1.0");
        beanConfig.setSchemes(new String[]{_useHttps ? CommonConstants.HTTPS_PROTOCOL : CommonConstants.HTTP_PROTOCOL});
        beanConfig.setBasePath("/");
        beanConfig.setResourcePackage(_controllerResourcePackages);
        beanConfig.setScan(true);

        ClassLoader loader = this.getClass().getClassLoader();
        URL swaggerDistLocation = loader.getResource(CommonConstants.CONFIG_OF_SWAGGER_RESOURCES_PATH);
        HttpHandler swaggerUiHandler = new SafeStaticHttpHandler(new URLClassLoader(new URL[]{swaggerDistLocation}), "/");

        _httpServer.getServerConfiguration().addHttpHandler(swaggerUiHandler, "/api/");
        _httpServer.getServerConfiguration().addHttpHandler(swaggerUiHandler, "/help/");
        _httpServer.getServerConfiguration().addHttpHandler(swaggerUiHandler, "/swaggerui-dist/");
    }

    public void stop() {
        if (!_httpServer.isStarted()) {
            return;
        }
        _httpServer.shutdownNow();
    }

    private class CorsFilter implements ContainerResponseFilter {
        @Override
        public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
            responseContext.getHeaders().add("Access-Control-Allow-Origin", "*");
            responseContext.getHeaders().add("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS, DELETE");
            responseContext.getHeaders().add("Access-Control-Allow-Headers", "*");
            if (requestContext.getMethod().equals("OPTIONS")) {
                responseContext.setStatus(HttpServletResponse.SC_OK);
            }
        }
    }

    public class SafeStaticHttpHandler extends CLStaticHttpHandler {
        public SafeStaticHttpHandler(ClassLoader classLoader, String... docRoots) {
            super(classLoader, docRoots);
        }

        @Override
        public void service(Request request, Response response) throws Exception {
            String uri = request.getRequestURI();
            if (uri.contains("../") || uri.contains("://") || uri.contains("%")) {
                response.setStatus(400);
                response.getWriter().write("Invalid request");
                return;
            }
            super.service(request, response);
        }
    }
}
