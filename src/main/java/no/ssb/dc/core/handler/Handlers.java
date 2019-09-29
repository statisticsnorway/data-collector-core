package no.ssb.dc.core.handler;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import no.ssb.dc.api.handler.CompositionHandler;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.BaseNode;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.isDeclaredBy;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.returns;

public class Handlers {

    private final Map<Class<? extends BaseNode>, Class<? extends AbstractHandler>> handlerFactory = new ConcurrentHashMap<>();
    private final Map<String, Class<? extends AbstractHandler>> compositionHandlerFactory = new ConcurrentHashMap<>();
    private static final boolean useByteBuddy = false;

    private static Handlers instance() {
        return NodeFactoryHolder.INSTANCE;
    }

    public static <N extends BaseNode> AbstractHandler<N> createHandlerFor(BaseNode node) {
        try {
            Class<? extends BaseNode> nodeClass = (Class<? extends BaseNode>) node.getClass().getInterfaces()[0];
            Class<? extends AbstractHandler> handlerClass = instance().handlerFactory.get(nodeClass);
            if (handlerClass == null) {
                throw new IllegalStateException("unable to resolve handler for: " + nodeClass);
            }

            if (useByteBuddy) {
                Class<? extends AbstractHandler> unloadedType = new ByteBuddy()
                        .subclass(handlerClass)
//                    .implement(Execution.class)
                        .method(named("execute").and(isDeclaredBy(ExecutionContext.class).and(returns(ExecutionContext.class))))
                        .intercept(to(new ExecutionWrapper()))
                        .make()
                        .load(handlerClass.getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                        .getLoaded();

                /*
                 * Create new node handler
                 */
                Constructor<? extends AbstractHandler> constructor = unloadedType.getDeclaredConstructor(nodeClass);
                return constructor.newInstance(node);

            } else {
                Constructor<? extends AbstractHandler> constructor = handlerClass.getDeclaredConstructor(nodeClass);
                return constructor.newInstance(node);
            }

        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public static <N extends BaseNode> AbstractHandler<N> createCompositionHandlerFor(BaseNode node,
                                                                                      Class<? extends BaseNode> forNodeClass,
                                                                                      Class<? extends BaseNode> selectorNodeClass) {
        try {
            String classes = createHandlerClasses(forNodeClass, selectorNodeClass);
            Class<? extends AbstractHandler> compositionHandlerClass = instance().compositionHandlerFactory.get(classes);
            if (compositionHandlerClass == null) {
                throw new IllegalStateException("unable to resolve handler for: " + forNodeClass);
            }
            /*
             * Create new node handler
             */
            Constructor<? extends AbstractHandler> constructor = compositionHandlerClass.getDeclaredConstructor(forNodeClass);
            return constructor.newInstance(node);

        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public void registerHandler(Class<? extends BaseNode> nodeClass, Class<? extends AbstractHandler> nodeHandlerClass) {
        handlerFactory.put(nodeClass, nodeHandlerClass);
    }

    public void registerCompositionHandler(String nodeClasses, Class<? extends AbstractHandler> nodeHandlerClass) {
        compositionHandlerFactory.put(nodeClasses, nodeHandlerClass);
    }

    private static final class NodeFactoryHolder {
        private static final Handlers INSTANCE = new Handlers();

        /*
         * Discover node handlers
         */
        static {
            nodeHandlerDiscovery(Handlers.class.getPackageName()).forEach(nodeHandlerClass -> {
                if (!(nodeHandlerClass.isAnnotationPresent(Handler.class) || nodeHandlerClass.isAnnotationPresent(CompositionHandler.class))) {
                    throw new RuntimeException("Handler is not declared for clazz: " + nodeHandlerClass);
                }

                if (nodeHandlerClass.isAnnotationPresent(Handler.class)) {
                    Class<? extends BaseNode> nodeClass = nodeHandlerClass.getAnnotation(Handler.class).forClass();
                    instance().registerHandler(nodeClass, nodeHandlerClass);
                }

                if (nodeHandlerClass.isAnnotationPresent(CompositionHandler.class)) {
                    CompositionHandler compositionHandlerAnnotation = nodeHandlerClass.getAnnotation(CompositionHandler.class);
                    Class<? extends BaseNode> nodeClass = compositionHandlerAnnotation.forClass();
                    Class<? extends BaseNode> byClass = compositionHandlerAnnotation.selectorClass();
                    String classes = createHandlerClasses(nodeClass, byClass);
                    instance().registerCompositionHandler(classes, nodeHandlerClass);
                }
            });
        }

        static Set<Class<? extends AbstractHandler>> nodeHandlerDiscovery(String... packageNames) {
            try (ScanResult scanResult = new ClassGraph().enableAnnotationInfo().whitelistPackages(packageNames).scan()) {
                ClassInfoList classInfoHandlerList = scanResult.getClassesWithAnnotation(Handler.class.getName());
                ClassInfoList classInfoCompositionHandlerList = scanResult.getClassesWithAnnotation(CompositionHandler.class.getName());
                Set<Class<? extends AbstractHandler>> classes = new HashSet<>();
                for (Class<?> clazz : classInfoHandlerList.loadClasses()) {
                    classes.add((Class<? extends AbstractHandler>) clazz);
                }
                for (Class<?> clazz : classInfoCompositionHandlerList.loadClasses()) {
                    classes.add((Class<? extends AbstractHandler>) clazz);
                }
                return classes;
            }
        }
    }

    private static String createHandlerClasses(Class<?> nodeClass, Class<?> byClass) {
        return nodeClass.getName() + " " + byClass.getName();
    }
}
