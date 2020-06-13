package no.ssb.dc.core.handler;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.el.ExpressionLanguage;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.node.BodyPublisherProducer;
import no.ssb.dc.api.node.JwtIdentity;
import no.ssb.dc.api.node.JwtTokenBodyPublisherProducer;
import no.ssb.dc.core.security.CertificateContext;
import no.ssb.dc.core.security.CertificateFactory;

import java.security.KeyPair;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Handler(forClass = JwtTokenBodyPublisherProducer.class)
public class JwtTokenBodyPublisherProducerHandler extends AbstractHandler<JwtTokenBodyPublisherProducer> {

    public JwtTokenBodyPublisherProducerHandler(JwtTokenBodyPublisherProducer node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        JwtIdentity jwtIdentity = (JwtIdentity) node.identity();

        JWTCreator.Builder jwtBuilder = JWT.create();

        CertificateFactory certificateFactory = context.services().get(CertificateFactory.class);
        CertificateContext certificateContext = certificateFactory.getCertificateContext(jwtIdentity.headerClaims().sslBundleName());

        KeyPair keyPair = certificateContext.keyPair();
        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
        RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();
        Algorithm algorithm = Algorithm.RSA256(publicKey, privateKey);

        Map<String, Object> headers = new LinkedHashMap<>();
        headers.put("alg", jwtIdentity.headerClaims().alg());
        headers.put("x5c", List.of(certificateContext.trustManager().getAcceptedIssuers()).stream().map(this::getEncodedCertificate).collect(Collectors.toList()));
        jwtBuilder.withHeader(headers);

        jwtBuilder.withIssuer(jwtIdentity.claims().issuer());
        jwtBuilder.withAudience(jwtIdentity.claims().audience());

        // custom claims
        for (Map.Entry<String, String> entry : jwtIdentity.claims().getClaims().entrySet()) {
            jwtBuilder.withClaim(entry.getKey(), entry.getValue());
        }

        long expirationInSeconds = jwtIdentity.claims().timeToLiveInSeconds();
        OffsetDateTime now = Instant.now().atOffset(ZoneOffset.UTC);
        jwtBuilder.withIssuedAt(Date.from(now.toInstant()));
        jwtBuilder.withExpiresAt(Date.from(now.plusSeconds(expirationInSeconds).toInstant()));
        jwtBuilder.withJWTId(UUID.randomUUID().toString());

        String token = jwtBuilder.sign(algorithm);

        ExecutionContext evalContext = ExecutionContext.of(context);
        evalContext.variable(node.bindTo(), token);

        String jwtGrant = evaluateExpression(evalContext, node.token());

        return ExecutionContext.empty().state(BodyPublisherProducer.class, jwtGrant.getBytes());
    }

    byte[] getEncodedCertificate(X509Certificate crt) {
        try {
            return crt.getEncoded();
        } catch (CertificateEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private String evaluateExpression(ExecutionContext context, String text) {
        ExpressionLanguage el = new ExpressionLanguage(context);
        if (el.isExpression(text)) {
            return el.evaluateExpressions(text);
        } else {
            return text;
        }
    }

}
