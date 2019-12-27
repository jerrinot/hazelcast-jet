package com.hazelcast.jet.spring;

public class SillyRiskCalculator implements RiskCalculator {
    public long calculateRisk(long i) {
        return -i;
    }
}
