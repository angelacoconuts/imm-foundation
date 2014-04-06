package com.enhype.extract;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class EntitySiteTuple {

	private String entity;
	private String siteId;
	
	public EntitySiteTuple(String entity, String siteId){
		this.entity = entity;
		this.siteId = siteId;
	}
	
	public String getSiteId() {
		return siteId;
	}
	public void setSiteId(String siteId) {
		this.siteId = siteId;
	}
	public String getEntity() {
		return entity;
	}
	public void setEntity(String entity) {
		this.entity = entity;
	}
	
	@Override
	public boolean equals(Object obj) {
        if (obj != null && obj instanceof EntitySiteTuple) {
        	EntitySiteTuple p = (EntitySiteTuple)obj;
            return ((entity==p.entity) && (siteId==p.siteId));
        }
        return false;
    }

	@Override
	public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
            // if deriving: appendSuper(super.hashCode()).
            append(entity).
            append(siteId).
            toHashCode();
    }
	
}
