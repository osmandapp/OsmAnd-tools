package net.osmand.server.api.operation;

import java.lang.reflect.RecordComponent;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.aop.support.AopUtils;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class OperationRegistry {

	public record ParamDescriptor(String name, String label, String type, boolean required, String defaultValue,
								  String helpText) {}
	public record OperationDescriptor(Operation<?> bean, Class<?> paramsType, List<ParamDescriptor> params) {}

	private final ApplicationContext applicationContext;
	private final OperationRepositoryConfiguration dbConfig;
	private final OperationRepository repository;
	private final ObjectMapper mapper;
	private final Map<String, OperationDescriptor> descriptors = new ConcurrentHashMap<>();

	public OperationRegistry(ApplicationContext applicationContext, OperationRepositoryConfiguration dbConfig,
							  OperationRepository repository, ObjectMapper mapper) {
		this.applicationContext = applicationContext;
		this.dbConfig = dbConfig;
		this.repository = repository;
		this.mapper = mapper;
	}

	@EventListener(ApplicationReadyEvent.class)
	public void onApplicationReady() {
		if (dbConfig.isOperationDataSourceInitialized()) {
			discover();
		}
	}

	public synchronized void discover() {
		repository.markAllOperationsInvalid();
		descriptors.clear();
		for (Operation<?> bean : applicationContext.getBeansOfType(Operation.class).values()) {
			Class<?> targetClass = AopUtils.getTargetClass(bean);
			AdminOperation annotation = targetClass.getAnnotation(AdminOperation.class);
			if (annotation == null) {
				continue;
			}
			Class<?> paramsType = ResolvableType.forClass(targetClass).as(Operation.class).getGeneric(0).resolve();
			List<ParamDescriptor> params = describeParams(paramsType);
			repository.upsertOperation(targetClass.getName(), annotation.name(),
					toJson(params), paramsType == null ? "" : paramsType.getName());
			descriptors.put(targetClass.getName(), new OperationDescriptor(bean, paramsType, params));
		}
		repository.deleteOrphanOperations();
	}

	public Optional<OperationDescriptor> resolve(String className) {
		return Optional.ofNullable(descriptors.get(className));
	}

	private List<ParamDescriptor> describeParams(Class<?> paramsType) {
		if (paramsType == null || !paramsType.isRecord()) {
			return List.of();
		}
		List<ParamDescriptor> params = new ArrayList<>();
		for (RecordComponent component : paramsType.getRecordComponents()) {
			String name = component.getName();
			params.add(new ParamDescriptor(name, name, inputType(component.getType()), false, "", ""));
		}
		return params;
	}

	private String inputType(Class<?> type) {
		if (type == boolean.class || type == Boolean.class) {
			return "checkbox";
		}
		if (type == char.class || type == Character.class || type == String.class) {
			return "text";
		}
		if (type == LocalDate.class || type == LocalDateTime.class || type == Date.class) {
			return "date";
		}
		if (type.isPrimitive() || Number.class.isAssignableFrom(type)) {
			return "number";
		}
		return "textarea";
	}

	private String toJson(Object value) {
		try {
			return mapper.writeValueAsString(value);
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to serialize operation parameters", e);
		}
	}
}
