class RatedEvent {

    private String eventType, targetResource, eventStartTime, receiver;
    private double eventUnitConsumed;

    String getEventType() {
        return eventType;
    }

    String getTargetResource() {
        return targetResource;
    }


    String getEventStartTime() {
        return eventStartTime;
    }

    String getReceiver() {
        return receiver;
    }

    double getEventUnitConsumed() {
        return eventUnitConsumed;
    }

    void setEventType(String eventType) {
        this.eventType = eventType;
    }

    void setTargetResource(String targetResource) {
        this.targetResource = targetResource;
    }


    void setEventStartTime(String eventStartTime) {
        this.eventStartTime = eventStartTime;
    }

    void setReceiver(String receiver) {
        this.receiver = receiver;
    }

    void setEventUnitConsumed(double eventUnitConsumed) {
        this.eventUnitConsumed = eventUnitConsumed;
    }
}
