package com.enhype.extract;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class FeatureSiteTuple {

	private String feature;
	private String siteId;
	
	public FeatureSiteTuple(String entity, String siteId){
		this.feature = entity;
		this.siteId = siteId;
	}
	
	public String getSiteId() {
		return siteId;
	}
	public void setSiteId(String siteId) {
		this.siteId = siteId;
	}
	public String getFeature() {
		return feature;
	}
	public void setFeature(String feature) {
		this.feature = feature;
	}
	
	@Override
	public boolean equals(Object obj) {
        if (obj != null && obj instanceof FeatureSiteTuple) {
        	FeatureSiteTuple p = (FeatureSiteTuple)obj;
            return ( feature.equals(p.feature) && siteId.equals(p.siteId) );
        }
        return false;
    }

	@Override
	public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
            // if deriving: appendSuper(super.hashCode()).
            append(feature).
            append(siteId).
            toHashCode();
    }
	
}
