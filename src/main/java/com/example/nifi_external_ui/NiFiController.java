package com.example.nifi_external_ui;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class NiFiController {

    private final NiFiService niFiService;

    public NiFiController(NiFiService niFiService) {
        this.niFiService = niFiService;
    }

    @GetMapping("/process-groups-list")
    @ResponseBody
    public Map<String, Object> getProcessGroupsList() {

        List<Map<String, Object>> list = niFiService.getAllProcessGroups();

        Map<String, Object> resp = new HashMap<>();
        resp.put("processGroups", list);

        return resp;
    }


    @PostMapping("/process-groups/{id}/start")
    @ResponseBody
    public String start(@PathVariable String id) {
        niFiService.startProcessGroup(id);
        return "Started";
    }

    @PostMapping("/process-groups/{id}/stop")
    @ResponseBody
    public String stop(@PathVariable String id) {
        niFiService.stopProcessGroup(id);
        return "Stopped";
    }

    @GetMapping("/parameter-contexts")
    @ResponseBody
    public List<Map<String, Object>> getAllParameterContexts() {
        return niFiService.getAllParameterContexts();
    }

    @PostMapping("/process-groups/{id}/set-parameter-context")
    public String setParameterContext(@PathVariable("id") String pgId,
                                      @RequestBody Map<String, String> body) {
        String pcId = body.get("pcId");
        if (pcId == null || pcId.isEmpty()) return "No parameter context selected";
        niFiService.setParameterContextForPG(pgId, pcId);
        return "Parameter context updated";
    }

    @PostMapping("/update-token")
    public String updateToken(@RequestBody Map<String, String> body) {
        String token = body.get("token");
        niFiService.updateAuthToken( token);
        return "Token updated";
    }

    @PostMapping("/nifi-test-mapping")
    public ResponseEntity<Object> nifiTestMapping(
            @RequestBody NifiTestMappingRequest request
    ) throws Exception {

        return ResponseEntity.ok(
                niFiService.buildPayload(
                        request.getFormInputStr(),
                        request.getAfterTaskData(),
                        request.getTicketId()
                )
        );
    }
}
