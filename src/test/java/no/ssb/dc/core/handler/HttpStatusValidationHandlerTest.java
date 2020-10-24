package no.ssb.dc.core.handler;

import no.ssb.dc.api.Builders;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.builder.BodyContainsBuilder;
import no.ssb.dc.api.node.builder.HttpStatusValidationBuilder;
import no.ssb.dc.api.node.builder.ResponsePredicateBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Flow;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HttpStatusValidationHandlerTest {

    static List<ValidatorPredicate> httpStatusValidationBuilderProvider() {
        List<ValidatorPredicate> validators = new ArrayList<>();
        validators.add(new ValidatorPredicate(200, null, null));
        validators.add(new ValidatorPredicate(404, "SM-001", new BodyContainsBuilder(Builders.xpath("/feil/kode"), "SM-001")));
        validators.add(new ValidatorPredicate(404, "SM-002", new BodyContainsBuilder(Builders.xpath("/feil/kode"), "SM-002")));
        return validators;
    }

    public static Response mockResponse(ValidatorPredicate validatorPredicate) {
        return new Response() {
            @Override
            public String url() {
                return "http://example.com";
            }

            @Override
            public Headers headers() {
                return new Headers();
            }

            @Override
            public int statusCode() {
                return validatorPredicate.statusCode;
            }

            @Override
            public byte[] body() {
                return validatorPredicate.statusCode == 200 ? new byte[0] :
                        ("<?xml version='1.0' encoding='UTF-8'?>" +
                                "<feil xmlns=\"urn:no:skatteetaten:datasamarbeid:feil:v1\">" +
                                "  <kode>" + validatorPredicate.errorCode + "</kode>" +
                                "</feil>"
                        ).getBytes();
            }

            @Override
            public Optional<Flow.Subscriber<List<ByteBuffer>>> bodyHandler() {
                return Optional.empty();
            }

            @Override
            public Optional<Response> previousResponse() {
                return Optional.empty();
            }
        };
    }

    @Test
    public void allValidators() {
        assertDoesNotThrow(() -> {
            HttpStatusValidationBuilder builder = new HttpStatusValidationBuilder();
            for (ValidatorPredicate validatorPredicate : httpStatusValidationBuilderProvider()) {
                if (validatorPredicate.builder == null) {
                    builder.success(validatorPredicate.statusCode);
                } else {
                    builder.success(validatorPredicate.statusCode, validatorPredicate.builder);
                }

                HttpStatusValidationHandler handler = new HttpStatusValidationHandler(builder.build());

                Response mockResponse = mockResponse(validatorPredicate);

                ExecutionContext context = ExecutionContext.empty().state(Response.class, mockResponse);
                handler.execute(context);
            }
        }, "Validator did not succeed with validation rules!");
    }

    @ParameterizedTest
    @MethodSource("httpStatusValidationBuilderProvider")
    public void eachValidator(ValidatorPredicate validatorPredicate) {
        assertDoesNotThrow(() -> {
            HttpStatusValidationBuilder builder = new HttpStatusValidationBuilder();
            if (validatorPredicate.builder == null) {
                builder.success(validatorPredicate.statusCode);
            } else {
                builder.success(validatorPredicate.statusCode, validatorPredicate.builder);
            }
            HttpStatusValidationHandler handler = new HttpStatusValidationHandler(builder.build());

            Response mockResponse = mockResponse(validatorPredicate);

            ExecutionContext context = ExecutionContext.empty().state(Response.class, mockResponse);
            handler.execute(context);

        }, "Validator did not succeed with validation rules!");
    }

    @Test
    public void validateSuccess() {
        assertDoesNotThrow(() -> {
            HttpStatusValidationBuilder builder = new HttpStatusValidationBuilder();
            builder.success(200, 299);
            HttpStatusValidationHandler handler = new HttpStatusValidationHandler(builder.build());
            handler.execute(ExecutionContext.empty().state(Response.class, mockResponse(new ValidatorPredicate(200, null, null))));
        }, "Validator did not succeed with 200");
    }

    @Test
    public void validateFailure() {
        assertThrows(HttpErrorException.class, () -> {
            HttpStatusValidationBuilder builder = new HttpStatusValidationBuilder();
            builder.success(200, 299);
            HttpStatusValidationHandler handler = new HttpStatusValidationHandler(builder.build());
            handler.execute(ExecutionContext.empty().state(Response.class, mockResponse(new ValidatorPredicate(500, null, null))));
        }, "Validator did not fail for 500");
    }

    static class ValidatorPredicate {
        final int statusCode;
        final String errorCode;
        final ResponsePredicateBuilder builder;

        ValidatorPredicate(int statusCode, String errorCode, ResponsePredicateBuilder builder) {
            this.statusCode = statusCode;
            this.errorCode = errorCode;
            this.builder = builder;
        }
    }
}
