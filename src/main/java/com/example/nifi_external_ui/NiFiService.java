package com.example.nifi_external_ui;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.*;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class NiFiService {

    private final RestTemplate restTemplate = new RestTemplate();
    private static final String NIFI_BASE_URL = "http://localhost:8080";
    private static final String NIFI_API_BASE = NIFI_BASE_URL + "/nifi-api";
    /**
     * Lấy thông tin Process Groups dưới root
     */
    public Map<String, Object> getProcessGroups() {
        String url = NIFI_API_BASE + "/flow/process-groups/root";
        return restTemplate.getForObject(url, Map.class);
    }
    public List<Map<String, Object>> getAllProcessGroups() {
        return getProcessGroupsRecursive("root");
    }
    public Map<String, Object> getProcessGroupFlow(String groupId) {
        String url = NIFI_API_BASE + "/flow/process-groups/" + groupId;
        return restTemplate.getForObject(url, Map.class);
    }

//    private List<Map<String, Object>> getProcessGroupsRecursive(String groupId) {
//        List<Map<String, Object>> result = new ArrayList<>();
//
//        Map<String, Object> flow = getProcessGroupFlow(groupId);
//
//        Map<String, Object> pgFlow = (Map<String, Object>) flow.get("processGroupFlow");
//        Map<String, Object> flowInner = (Map<String, Object>) pgFlow.get("flow");
//
//        List<Map<String, Object>> groups = (List<Map<String, Object>>) flowInner.get("processGroups");
//
//        if (groups == null) return result;
//
//        for (Map<String, Object> g : groups) {
//            Map<String, Object> component = (Map<String, Object>) g.get("component");
//            if (component != null) {
//                Map<String, Object> item = new HashMap<>();
//                item.put("id", component.get("id"));
//                item.put("name", component.get("name"));
//                result.add(item);
//            }
//
//            // lấy id của group con
//            if (component != null && component.get("id") != null) {
//                String childId = (String) component.get("id");
//
//                // tiếp tục đệ quy
//                result.addAll(getProcessGroupsRecursive(childId));
//            }
//        }
//
//        return result;
//    }
private List<Map<String, Object>> getProcessGroupsRecursive(String groupId) {

    List<Map<String, Object>> result = new ArrayList<>();

    // lấy flow từ NiFi
    Map<String, Object> flow = getProcessGroupFlow(groupId);
    if (flow == null) return result;

    Map<String, Object> pgFlow = (Map<String, Object>) flow.get("processGroupFlow");
    if (pgFlow == null) return result;

    Map<String, Object> flowInner = (Map<String, Object>) pgFlow.get("flow");
    if (flowInner == null) return result;

    List<Map<String, Object>> groups =
            (List<Map<String, Object>>) flowInner.get("processGroups");

    if (groups == null || groups.isEmpty()) return result;

    for (Map<String, Object> g : groups) {

        Map<String, Object> component = (Map<String, Object>) g.get("component");
        if (component == null) continue;

        String id = (String) component.get("id");
        String name = (String) component.get("name");

        if (id != null && name != null) {
            Map<String, Object> item = new HashMap<>();
            item.put("id", id);
            item.put("name", name);
            result.add(item);
        }

        // recursive lấy group con
        if (id != null) {
            result.addAll(getProcessGroupsRecursive(id));
        }
    }

    return result;
}

    /**
     * Start Remote Process Group bằng trạng thái TRANSMITTING
     */
    public void startProcessGroup(String rpgId) {
        updateRemoteProcessGroupState(rpgId, "RUNNING");
    }

    /**
     * Stop Remote Process Group (ví dụ STOPPED)
     */
    public void stopProcessGroup(String rpgId) {
        updateRemoteProcessGroupState(rpgId, "STOPPED");
    }

    /**
     * Cập nhật trạng thái cho Remote Process Group
     */


    private void updateRemoteProcessGroupState(String pgId, String status) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.add("Accept", "application/json, text/javascript, */*; q=0.01");
            headers.add("X-Requested-With", "XMLHttpRequest");
            headers.add("Request-Token", "__Secure-Request-Token"); // token thật

            // Sử dụng biến cho Origin & Referer
            headers.add("Origin", NIFI_BASE_URL);
            headers.add("Referer", NIFI_BASE_URL + "/nifi/?processGroupId=" + pgId + "&componentIds=");

            // 1. Request đầu tiên
            Map<String, Object> body1 = new HashMap<>();
            body1.put("id", pgId);
            body1.put("state", status);
            body1.put("disconnectedNodeAcknowledged", false);

            String url1 = NIFI_API_BASE + "/flow/process-groups/" + pgId;
            printCurl(url1, headers, body1);
            restTemplate.put(url1, new HttpEntity<>(body1, headers));
            System.out.println("Process Group " + pgId + " is now RUNNING");

            // 2. Request thứ hai
            Map<String, Object> body2 = new HashMap<>();
            body2.put("state", "TRANSMITTING");
            body2.put("disconnectedNodeAcknowledged", false);

            String url2 = NIFI_API_BASE + "/remote-process-groups/process-group/" + pgId + "/run-status";
            printCurl(url2, headers, body2);
            restTemplate.put(url2, new HttpEntity<>(body2, headers));
            System.out.println("Remote Process Group " + pgId + " is now TRANSMITTING");

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to update Process Groups", e);
        }
    }


    private void printCurl(String url, HttpHeaders headers, Object body) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(body);

            StringBuilder curl = new StringBuilder();
            curl.append("curl -X PUT '").append(url).append("' ");

            headers.forEach((k, vList) -> vList.forEach(v -> curl.append("-H '").append(k).append(": ").append(v).append("' ")));

            curl.append("--data-raw '").append(json.replace("'", "'\\''")).append("'");

            System.out.println("\n==== CURL GENERATED ====\n" + curl + "\n========================\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Lấy ParameterContextId từ Process Group
     */
//    public List<Map<String, Object>> getAllParameterContexts() {
//        String url = NIFI_API_BASE + "/flow/parameter-contexts";
//
//        HttpHeaders headers = new HttpHeaders();
//        headers.set("Accept", "application/json, text/javascript, */*; q=0.01");
//        headers.set("X-Requested-With", "XMLHttpRequest");
//
//        // Nếu NiFi của bạn có token thì thêm
//        headers.set("Request-Token", "__Secure-Request-Token");  // thay token thật
//
//        HttpEntity<Void> entity = new HttpEntity<>(headers);
//
//        ResponseEntity<Map> response =
//                restTemplate.exchange(url, HttpMethod.GET, entity, Map.class);
//
//        Map<String, Object> body = response.getBody();
//        if (body == null) return List.of();
//
//        return (List<Map<String, Object>>) body.get("parameterContexts");
//    }
    public List<Map<String, Object>> getAllParameterContexts() {
        String url = NIFI_API_BASE + "/flow/parameter-contexts";

        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", "application/json, text/javascript, */*; q=0.01");
        headers.set("X-Requested-With", "XMLHttpRequest");
        headers.set("Request-Token", "__Secure-Request-Token"); // thay token thật

        HttpEntity<Void> entity = new HttpEntity<>(headers);
        ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.GET, entity, Map.class);

        Map<String, Object> body = response.getBody();
        if (body == null) return List.of();

        List<Map<String, Object>> parameterContexts =
                (List<Map<String, Object>>) body.get("parameterContexts");

        // Gán thông tin PG đang dùng vào mỗi context
        parameterContexts.forEach(ctx -> {
            List<Map<String, Object>> boundPGs = (List<Map<String, Object>>)
                    ((Map<String, Object>) ctx.get("component")).get("boundProcessGroups");

            // Lấy id và tên PG đang dùng context, có thể nhiều PG
            List<String> pgNames = boundPGs.stream()
                    .map(pg -> (String) ((Map<String, Object>) pg.get("component")).get("name"))
                    .collect(Collectors.toList());

            ctx.put("usedByPGs", pgNames); // thêm key mới
        });

        return parameterContexts;
    }
    public String getParameterContextsByPGId(String pgId) {
        String url = NIFI_API_BASE + "/flow/process-groups/"+pgId+"?uiOnly=true";

        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", "application/json, text/javascript, */*; q=0.01");
        headers.set("X-Requested-With", "XMLHttpRequest");

        // Nếu NiFi của bạn có token thì thêm
        headers.set("Request-Token", "__Secure-Request-Token");  // thay token thật

        HttpEntity<Void> entity = new HttpEntity<>(headers);

        ResponseEntity<Map> response =
                restTemplate.exchange(url, HttpMethod.GET, entity, Map.class);

        Map<String, Object> body = response.getBody();
        if (body == null) return null;
        Map<String, Object> processGroupFlow = (Map<String, Object>) body.get("processGroupFlow");
        if (processGroupFlow == null) return "";

        Map<String, Object> parameterContext = (Map<String, Object>) processGroupFlow.get("parameterContext");
        if (parameterContext == null) return "";

        Map<String, Object> component = (Map<String, Object>) parameterContext.get("component");
        if (component == null) return "";

//        Map<String, Object> pcComponent = (Map<String, Object>) parameterContext.get("component");
//        if (pcComponent == null) return "";

// Trả về tên parameter context
        return (String) component.get("name");

    }
//    public void setParameterContextForPG(String pgId, String pcId) {
//        Map<String, Object> pgFlow = getProcessGroupFlow(pgId);
//
//        if (pgFlow == null) throw new RuntimeException("Cannot get PG flow for " + pgId);
//
//        Map<String,Object> pgFlowMap = (Map<String,Object>) pgFlow.get("processGroupFlow");
//        Map<String,Object> component = (Map<String,Object>) pgFlowMap.get("component");
//        Map<String,Object> revisionInfo = (Map<String,Object>) pgFlow.get("revision");
//
//        Map<String,Object> revision = new HashMap<>();
//        revision.put("clientId", revisionInfo.get("clientId"));
//        revision.put("version", revisionInfo.get("version"));
//
//        Map<String,Object> paramContextMap = new HashMap<>();
//        paramContextMap.put("id", pcId);
//        component.put("parameterContext", paramContextMap);
//
//        Map<String,Object> body = new HashMap<>();
//        body.put("revision", revision);
//        body.put("disconnectedNodeAcknowledged", false);
//        body.put("processGroupUpdateStrategy", "DIRECT_CHILDREN");
//        body.put("component", component);
//
//        HttpHeaders headers = new HttpHeaders();
//        headers.setContentType(MediaType.APPLICATION_JSON);
//        headers.add("X-Requested-With", "XMLHttpRequest");
//        headers.add("Request-Token", "__Secure-Request-Token"); // token thật
//
//        String url = NIFI_API_BASE + "/process-groups/" + pgId;
//        restTemplate.put(url, new HttpEntity<>(body, headers));
//    }
public void setParameterContextForPG(String pgId, String pcId) {
    // 1. Lấy PG info từ ?uiOnly=true để có revision hợp lệ
    String urlGet = NIFI_API_BASE + "/process-groups/" + pgId + "?uiOnly=true";
    HttpHeaders headersGet = new HttpHeaders();
    headersGet.set("Accept", "application/json, text/javascript, */*; q=0.01");
    headersGet.set("X-Requested-With", "XMLHttpRequest");
    headersGet.set("Request-Token", "__Secure-Request-Token"); // token thật
    Map<String, Object> pgFlow = restTemplate.exchange(urlGet, HttpMethod.GET,
            new HttpEntity<>(headersGet), Map.class).getBody();

    if (pgFlow == null) throw new RuntimeException("Cannot get PG flow for " + pgId);

    Map<String, Object> component = (Map<String, Object>) ((Map<String, Object>) pgFlow.get("component"));
    Map<String, Object> revisionInfo = (Map<String, Object>) pgFlow.get("revision");

    if (component == null || revisionInfo == null)
        throw new RuntimeException("Missing component or revision info for PG " + pgId);

    // 2. Tạo revision mới
    Map<String, Object> revision = new HashMap<>();
    revision.put("clientId", revisionInfo.get("clientId"));
    revision.put("version", revisionInfo.get("version"));

    // 3. Gán ParameterContext mới
    Map<String, Object> paramContextMap = new HashMap<>();
    paramContextMap.put("id", pcId);
    component.put("parameterContext", paramContextMap);

    // 4. Tạo body PUT
    Map<String, Object> body = new HashMap<>();
    body.put("revision", revision);
    body.put("disconnectedNodeAcknowledged", false);
    body.put("processGroupUpdateStrategy", "DIRECT_CHILDREN");
    body.put("component", component);

    // 5. PUT lên NiFi
    HttpHeaders headersPut = new HttpHeaders();
    headersPut.setContentType(MediaType.APPLICATION_JSON);
    headersPut.add("X-Requested-With", "XMLHttpRequest");
    headersPut.add("Request-Token", "__Secure-Request-Token"); // token thật

    String urlPut = NIFI_API_BASE + "/process-groups/" + pgId;
    restTemplate.put(urlPut, new HttpEntity<>(body, headersPut));
}

    public void updateAuthToken( String newToken) {
        RestTemplate rest = new RestTemplate();
        String contextId = getPcIdByName("AuthContextKyta");
        // 1. Lấy current revision
        JsonNode ctx = rest.getForObject(
                NIFI_API_BASE + "/parameter-contexts/" + contextId,
                JsonNode.class
        );

        int version = ctx.get("revision").get("version").asInt();

        // 2. Build body
        ObjectNode body = JsonNodeFactory.instance.objectNode();

        ObjectNode revision = body.putObject("revision");
        revision.put("clientId", UUID.randomUUID().toString());
        revision.put("version", version);

        body.put("id", contextId);

        ObjectNode comp = body.putObject("component");
        comp.put("id", contextId);
        comp.put("name", ctx.get("component").get("name").asText());

        ArrayNode params = comp.putArray("parameters");

        ObjectNode paramNode = params.addObject();
        ObjectNode param = paramNode.putObject("parameter");
        param.put("name", "auth_token");
        param.put("value", newToken);
        param.put("sensitive", false);

        // 3. Gọi API update
        rest.postForObject(
                NIFI_API_BASE + "/parameter-contexts/" + contextId + "/update-requests",
                body,
                String.class
        );
    }


    public String getPcIdByName(String contextName) {
        List<Map<String, Object>> contexts = getAllParameterContexts();

        for (Map<String, Object> ctx : contexts) {
            Map<String, Object> component = (Map<String, Object>) ctx.get("component");
            if (component == null) continue;

            String name = (String) component.get("name");

            if (contextName.equals(name)) {
                return (String) component.get("id");   // ⬅ chính là pcId
            }
        }

        throw new RuntimeException("Không tìm thấy Parameter Context tên: " + contextName);
    }

}
