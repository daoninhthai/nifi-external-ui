package com.example.nifi_external_ui;




public class NifiTestMappingRequest {
    private Object formInputStr;
    private AfterTaskData afterTaskData;
    private String ticketId;

    public Object getFormInputStr() {
        return formInputStr;
    }

    public void setFormInputStr(Object formInputStr) {
        this.formInputStr = formInputStr;
    }

    public AfterTaskData getAfterTaskData() {
        return afterTaskData;
    }

    public void setAfterTaskData(AfterTaskData afterTaskData) {
        this.afterTaskData = afterTaskData;
    }

    public String getTicketId() {
        return ticketId;
    }

    public void setTicketId(String ticketId) {
        this.ticketId = ticketId;
    }
}
