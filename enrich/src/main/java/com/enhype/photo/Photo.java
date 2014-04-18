package com.enhype.photo;

public class Photo {
	
	private String id;
	private String owner;
	private String secret;
	private String title;
	private String url;

	public Photo(String id, String owner, String secret, String ServerId, String farmId, String title){
		
		this.setUrl("http://farm" + farmId + ".staticflickr.com/" + ServerId + "/" + id + "_" + secret + ".jpg");
		this.setId(id);
		this.setOwner(owner);
		this.setSecret(secret);
		this.setTitle(title);
		
	}

	public String getId() {
		return id;
	}

	private void setId(String id) {
		this.id = id;
	}

	public String getOwner() {
		return owner;
	}

	private void setOwner(String owner) {
		this.owner = owner;
	}

	public String getSecret() {
		return secret;
	}

	private void setSecret(String secret) {
		this.secret = secret;
	}

	public String getTitle() {
		return title;
	}

	private void setTitle(String title) {
		this.title = title;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
}
