package net.osmand.server.controllers.pub;

import net.osmand.util.Algorithms;
import org.apache.commons.codec.digest.HmacAlgorithms;
import org.apache.commons.codec.digest.HmacUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.MethodParameter;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.RequestBodyAdviceAdapter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

// https://developer.fastspring.com/docs/message-security
@Order(Ordered.HIGHEST_PRECEDENCE)
@ControllerAdvice(assignableTypes = FastSpringController.class)
public class FastSpringSignatureAdvice extends RequestBodyAdviceAdapter {

	private static final String SIGNATURE_HEADER = "X-FS-Signature";

	@Value("${FASTSPRING_WEBHOOK_SECRET:}")
	private String webhookSecret;

	@Override
	public boolean supports(MethodParameter parameter, Type targetType,
	                        Class<? extends HttpMessageConverter<?>> converterType) {
		return true;
	}

	@Override
	public HttpInputMessage beforeBodyRead(HttpInputMessage inputMessage, MethodParameter parameter, Type targetType,
	                                       Class<? extends HttpMessageConverter<?>> converterType) throws IOException {
		if (Algorithms.isEmpty(webhookSecret)) {
			throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
		}
		byte[] body = inputMessage.getBody().readAllBytes();
		if (!isValidSignature(body, inputMessage.getHeaders().getFirst(SIGNATURE_HEADER))) {
			throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
		}
		return new HttpInputMessage() {
			@Override
			public InputStream getBody() {
				return new ByteArrayInputStream(body);
			}

			@Override
			public HttpHeaders getHeaders() {
				return inputMessage.getHeaders();
			}
		};
	}

	@ExceptionHandler(ResponseStatusException.class)
	public ResponseEntity<Void> rejectedWebhook(ResponseStatusException e) {
		return ResponseEntity.status(e.getStatusCode()).build();
	}

	private boolean isValidSignature(byte[] body, String signature) {
		if (signature == null) {
			return false;
		}
		byte[] expected = Base64.getEncoder().encode(new HmacUtils(HmacAlgorithms.HMAC_SHA_256, webhookSecret).hmac(body));
		return MessageDigest.isEqual(expected, signature.getBytes(StandardCharsets.UTF_8));
	}
}
