package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.concurrent.Executor;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(tags = Constants.CLUSTER_TAG)
@Path("/")
public class HelloWorldResource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(org.apache.pinot.controller.api.resources.TableDebugResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  @Inject
  Executor _executor;

  @Inject
  HttpConnectionManager _connectionManager;

  @Inject
  ControllerMetrics _controllerMetrics;

  @Inject
  ControllerConf _controllerConf;

  @GET
  @Path("/helloWorld")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Hello World", notes = "Hello World")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 404, message = "Table not found"), @ApiResponse(code = 500, message = "Internal server error")})
  public String getTableDebugInfo()
      throws JsonProcessingException {

    ObjectNode root = JsonUtils.newObjectNode();
    root.put("helloWorld", _pinotHelixResourceManager.getHelixClusterName());
    return JsonUtils.DEFAULT_PRETTY_WRITER.writeValueAsString(root);
  }
}
