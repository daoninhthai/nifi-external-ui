package com.example.nifi_external_ui;

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

//    @GetMapping("/")
//    public String index(Model model) {
//        Map<String, Object> flow = niFiService.getProcessGroups();
//        // flow.get("processGroupFlow") -> Map
//        Map<String, Object> pgFlow = (Map<String, Object>) flow.get("processGroupFlow");
//        Map<String, Object> flowInner = (Map<String, Object>) pgFlow.get("flow");
//        List<Map<String,Object>> processGroups = (List<Map<String,Object>>) flowInner.get("processGroups");
//
//        model.addAttribute("processGroups", processGroups);
//        return "index";
//    }

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

}
