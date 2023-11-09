package net.osmand.server.api.services;

import net.osmand.render.RenderingRule;
import net.osmand.render.RenderingRuleProperty;
import net.osmand.render.RenderingRuleSearchRequest;
import net.osmand.render.RenderingRulesStorage;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class MapResourcesService {
    
    public Map<String, List<Map<String, String>>> parseAttributes(RenderingRulesStorage storage, List<String> attributes) {
        Map<String, List<Map<String, String>>> res = new HashMap<>();
        for (String attribute : attributes) {
            List<Map<String, String>> attributeStyles = parseRules(storage, attribute);
            if (attributeStyles != null) {
                res.put(attribute, attributeStyles);
            }
        }
        return res;
    }
    
    private List<Map<String, String>> parseRules(RenderingRulesStorage storage, String attribute) {
        List<RenderingRule> allRules = new ArrayList<>();
        RenderingRule rule = storage.getRenderingAttributeRule(attribute);
        if (rule != null) {
            allRules = getRules(rule, allRules);
            List<Map<String, String>> attributeStyles = new ArrayList<>();
            for (RenderingRule renderingRule : allRules) {
                RenderingRuleSearchRequest searchRequest = new RenderingRuleSearchRequest(storage);
                searchRequest.loadOutputProperties(renderingRule, true);
                Map<String, String> res = new HashMap<>();
                for (RenderingRuleProperty prop : renderingRule.getProperties()) {
                    String name = prop.getAttrName();
                    String value = getProperty(prop, name, searchRequest, renderingRule);
                    if (value != null) {
                        res.put(name, value);
                    }
                }
                if (!res.isEmpty()) {
                    attributeStyles.add(res);
                }
            }
            return attributeStyles;
        }
        return null;
    }
    
    private String getProperty(RenderingRuleProperty prop, String name, RenderingRuleSearchRequest searchRequest, RenderingRule renderingRule) {
        String value = null;
        if (prop.isString()) {
            value = searchRequest.getStringPropertyValue(prop);
            if (value == null) {
                value = renderingRule.getStringPropertyValue(name);
            }
        } else if (prop.isFloat()) {
            float f = searchRequest.getFloatPropertyValue(prop);
            value = f != 0 ? String.valueOf(f) : null;
            if (value == null) {
                f = renderingRule.getFloatPropertyValue(name);
                value = f != 0 ? String.valueOf(f) : null;
            }
        } else if (prop.isColor()) {
            value = searchRequest.getColorStringPropertyValue(prop);
            if (value == null) {
                value = renderingRule.getColorPropertyValue(name);
            }
        } else if (prop.isIntParse()) {
            int i = searchRequest.getIntPropertyValue(prop);
            value = i != -1 ? String.valueOf(i) : null;
            if (value == null) {
                i = renderingRule.getIntPropertyValue(name);
                value = i != -1 ? String.valueOf(i) : null;
            }
        }
        return value;
    }
    
    private List<RenderingRule> getRules(RenderingRule rule, List<RenderingRule> allRules) {
        allRules.add(rule);
        List<RenderingRule> ifElse = rule.getIfElseChildren();
        List<RenderingRule> ifChildren = rule.getIfChildren();
        List<RenderingRule> newList = new ArrayList<>();
        newList.addAll(ifElse);
        newList.addAll(ifChildren);
        if (!newList.isEmpty()) {
            for (RenderingRule r : newList) {
                allRules = getRules(r, allRules);
            }
        }
        return allRules;
    }
}
