/*-
 * ===============LICENSE_START=======================================================
 * Acumos
 * ===================================================================================
 * Copyright (C) 2017 AT&T Intellectual Property & Tech Mahindra. All rights reserved.
 * ===================================================================================
 * This Acumos software file is distributed by AT&T and Tech Mahindra
 * under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ===============LICENSE_END=========================================================
 */

package org.acumos.modelrunner.domain;

import java.util.ArrayList;

public class MessageObject {
	public class AttributeEntity {
		String name;
		String type;
		boolean isRepeated;
		boolean isRequired;

		public AttributeEntity(String name, String type, boolean isRepeated, boolean isRequired) {
			super();
			this.name = name;
			this.type = type;
			this.isRepeated = isRepeated;
			this.isRequired = isRequired;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public boolean isRepeated() {
			return isRepeated;
		}

		public void setRepeated(boolean isRepeated) {
			this.isRepeated = isRepeated;
		}
		
		public boolean isRequired() {
			return isRequired;
		}

		public void setRequired(boolean isRequired) {
			this.isRequired = isRequired;
		}

	}

	private String cname;
	private boolean isEnum;
	private Class<?> cls;
	private ArrayList<AttributeEntity> attributes;

	public MessageObject(String cname) {
		super();
		this.cname = cname;
		attributes = new ArrayList<AttributeEntity>();
	}

	public String getCname() {
		return cname;
	}

	public void setCname(String cname) {
		this.cname = cname;
	}
	
	public boolean isEnum() {
		return isEnum;
	}

	public void setEnum(boolean isEnum) {
		this.isEnum = isEnum;
	}

	public Class<?> getCls() {
		return cls;
	}

	public void setCls(Class<?> cls) {
		this.cls = cls;
	}

	public ArrayList<AttributeEntity> getAttributes() {
		return attributes;
	}

	public void addAttribute(AttributeEntity attribute) {
		this.attributes.add(attribute);
	}
	
	public void addAttribute(String attributeName, String attributeType, boolean isRepeated, boolean isRequired) {
		 AttributeEntity attribute = new AttributeEntity(attributeName, attributeType, isRepeated, isRequired);
		 this.attributes.add(attribute);
	}
}
