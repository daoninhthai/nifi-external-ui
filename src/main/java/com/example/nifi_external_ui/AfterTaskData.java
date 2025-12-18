package com.example.nifi_external_ui;

import java.io.Serializable;

public class AfterTaskData implements Serializable {
    private static final long serialVersionUID = 1L;

    private String id;
    private String service;
    private String selector;
    private String status;
    private String serviceTaskId;
    private String modelerId;
    private String orgIn;
    private String custId;
    private String userTaskIds;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getServiceTaskId() {
        return serviceTaskId;
    }

    public void setServiceTaskId(String serviceTaskId) {
        this.serviceTaskId = serviceTaskId;
    }

    public String getModelerId() {
        return modelerId;
    }

    public void setModelerId(String modelerId) {
        this.modelerId = modelerId;
    }

    public String getOrgIn() {
        return orgIn;
    }

    public void setOrgIn(String orgIn) {
        this.orgIn = orgIn;
    }

    public String getCustId() {
        return custId;
    }

    public void setCustId(String custId) {
        this.custId = custId;
    }

    public String getUserTaskIds() {
        return userTaskIds;
    }

    public void setUserTaskIds(String userTaskIds) {
        this.userTaskIds = userTaskIds;
    }
}
