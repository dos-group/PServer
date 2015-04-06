package de.tuberlin.pserver.app;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.utils.Compressor;
import org.apache.bcel.Repository;
import org.apache.bcel.classfile.*;
import org.apache.commons.lang3.tuple.Triple;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.*;

public final class UserCodeManager {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private static final class DependencyEmitter extends EmptyVisitor {

        private final JavaClass javaClass;

        private final List<String> dependencies;

        public DependencyEmitter(final JavaClass javaClass) {
            this.javaClass = javaClass;
            this.dependencies = new ArrayList<>();
        }

        @Override
        public void visitConstantClass(final ConstantClass obj) {
            final ConstantPool cp = javaClass.getConstantPool();
            String bytes = obj.getBytes(cp);
            dependencies.add(bytes);
        }

        public static List<String> analyze(final Class<?> clazz) {
            final JavaClass javaClass = Repository.lookupClass(clazz);
            final DependencyEmitter visitor = new DependencyEmitter(javaClass);
            (new DescendingVisitor(javaClass, visitor)).visit();
            return visitor.dependencies;
        }
    }

    // ---------------------------------------------------

    private static final class DynamicClassLoader extends ClassLoader {

        final Map<String, Class<?>> loadedClazzMap;

        public DynamicClassLoader(final ClassLoader cl) {
            super(cl);
            this.loadedClazzMap = new HashMap<>();
        }

        public Class<?> buildClassFromByteArray(final String clazzName, final byte[] clazzData) {
            Preconditions.checkNotNull(clazzName);
            Preconditions.checkNotNull(clazzData);
            if (!loadedClazzMap.containsKey(clazzName)) {
                final Class<?> newClazz;
                try {
                    newClazz = this.defineClass(clazzName, clazzData, 0, clazzData.length);
                } catch (ClassFormatError e) {
                    throw new IllegalStateException(e);
                }
                loadedClazzMap.put(clazzName, newClazz);
                return newClazz;
            } else
                return loadedClazzMap.get(clazzName);
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final List<String> standardDependencies;

    private final boolean analyseDependencies;

    private final DynamicClassLoader classLoader;

    private final Compressor compressor = Compressor.Factory.create(Compressor.CompressionType.JAVA_COMPRESSION);

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public UserCodeManager(final ClassLoader cl, boolean analyseDependencies) {
        this.standardDependencies = new ArrayList<>();
        this.analyseDependencies = analyseDependencies;
        this.classLoader = new DynamicClassLoader(Preconditions.checkNotNull(cl));
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public UserCodeManager addStandardDependency(final String path) {
        this.standardDependencies.add(Preconditions.checkNotNull(path));
        return this;
    }

    public Triple<Class<?>, List<String>, byte[]> extractClass(final Class<?> clazz) {
        Preconditions.checkNotNull(clazz);
        if (clazz.isMemberClass() && !Modifier.isStatic(clazz.getModifiers()))
            throw new IllegalStateException();
        final List<String> dependencies = analyseDependencies
                ? buildTransitiveDependencyClosure(clazz, new ArrayList<>())
                : new ArrayList<>();
        return Triple.of(clazz, dependencies, compressor.compress(loadByteCode(clazz)));
    }

    public Class<?> implantClass(final PServerJobDescriptor userCode) {
        Preconditions.checkNotNull(userCode);
        try {
            for (final String dependency : userCode.classDependencies)
                Class.forName(dependency, false, classLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
        Class<?> clazz;
        try {
            clazz = Class.forName(userCode.className);
        } catch (ClassNotFoundException e) {
            clazz = classLoader.buildClassFromByteArray(userCode.className, compressor.decompress(userCode.classByteCode));
        }
        return clazz;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private List<String> buildTransitiveDependencyClosure(final Class<?> clazz, final List<String> globalDependencies) {
        final String fullQualifiedPath = clazz.getCanonicalName();
        final List<String> levelDependencies = DependencyEmitter.analyze(clazz);
        for (String dependency : levelDependencies) {
            boolean isNewDependency = true;
            for (final String sd : standardDependencies)
                isNewDependency &= !dependency.contains(sd);
            if (isNewDependency) {
                final String dp1 = dependency.replace("/", ".");
                final String dp2 = dp1.replace("$", ".");
                boolean isTransitiveEnclosingClass = false;
                for (final String dp : globalDependencies)
                    if (dp.contains(dp2)) {
                        isTransitiveEnclosingClass = true;
                        break;
                    }
                if (!fullQualifiedPath.contains(dp2) && !isTransitiveEnclosingClass) {
                    globalDependencies.add(dp2);
                    final Class<?> dependencyClass;
                    try {
                        dependencyClass = Class.forName(dp1, false, clazz.getClassLoader());
                    } catch (ClassNotFoundException e) {
                        throw new IllegalStateException(e);
                    }
                    if (!dependencyClass.isArray() && !dependencyClass.isPrimitive())
                        buildTransitiveDependencyClosure(dependencyClass, globalDependencies);
                }
            }
        }

        return globalDependencies;
    }

    private byte[] loadByteCode(final Class<?> clazz) {
        String topLevelClazzName = null;
        Class<?> enclosingClazz = clazz.getEnclosingClass();
        while (enclosingClazz != null) {
            topLevelClazzName = enclosingClazz.getSimpleName();
            enclosingClazz = enclosingClazz.getEnclosingClass();
        }
        if (topLevelClazzName == null)
            topLevelClazzName = clazz.getSimpleName();

        final StringTokenizer tokenizer = new StringTokenizer(clazz.getCanonicalName(), ".");
        final StringBuilder pathBuilder = new StringBuilder();
        boolean isClazzFilename = false;
        while (tokenizer.hasMoreTokens()) {
            final String token = tokenizer.nextToken();
            if (!token.equals(topLevelClazzName) && !isClazzFilename)
                pathBuilder.append(token).append("/");
            else {
                pathBuilder.append(token);
                if (tokenizer.hasMoreTokens())
                    pathBuilder.append("$");
                isClazzFilename = true;
            }
        }

        final String filePath = clazz.getProtectionDomain().getCodeSource().getLocation().getPath() + pathBuilder.toString() + ".class";
        final File clazzFile = new File(filePath.replace("%20", " "));
        FileInputStream fis = null;
        byte[] clazzData = null;
        try {
            fis = new FileInputStream(clazzFile);
            clazzData = new byte[(int) clazzFile.length()]; // TODO: use IOUtils, Apache Commons!
            fis.read(clazzData);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                if (fis != null)
                    fis.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return clazzData;
    }
}