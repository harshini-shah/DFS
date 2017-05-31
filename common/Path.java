package common;

import java.io.*;
import java.util.*;

/** Distributed filesystem paths.

    <p>
    Objects of type <code>Path</code> are used by all filesystem interfaces.
    Path objects are immutable.

    <p>
    The string representation of paths is a forward-slash-delimeted sequence of
    path components. The root directory is represented as a single forward
    slash.

    <p>
    The colon (<code>:</code>) and forward slash (<code>/</code>) characters are
    not permitted within path components. The forward slash is the delimeter,
    and the colon is reserved as a delimeter for application use.
 */
public class Path implements Iterable<String>, Serializable, Comparable<Path>
{
    private static final long serialVersionUID = 42L;
    private List<String> pathComponents;
    
    /** Creates a new path which represents the root directory. */
    public Path()
    {
        pathComponents = new ArrayList<String>();
    }

    /** Creates a new path by appending the given component to an existing path.

        @param path The existing path.
        @param component The new component.
        @throws IllegalArgumentException If <code>component</code> includes the
                                         separator, a colon, or
                                         <code>component</code> is the empty
                                         string.
    */
    public Path(Path path, String component)
    {
        if (component == null || component.isEmpty()) {
            throw new IllegalArgumentException("The component is null/empty");
        } else if (component.contains(":")) {
            throw new IllegalArgumentException("Component contains : which is not allowed");
        } else if (component.contains("/")) {
            throw new IllegalArgumentException("Component contains / which is not allowed");
        }
        
        pathComponents = new ArrayList<String>(path.getPathComponents());
        pathComponents.add(component);
    }

    /** Creates a new path from a path string.

        <p>
        The string is a sequence of components delimited with forward slashes.
        Empty components are dropped. The string must begin with a forward
        slash.

        @param path The path string.
        @throws IllegalArgumentException If the path string does not begin with
                                         a forward slash, or if the path
                                         contains a colon character.
     */
    public Path(String path)
    {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("The path speicified is null or emmpty");
        } else if (path.charAt(0) != '/') {
            throw new IllegalArgumentException("The path does not begin with a forward slash");
        } else if (path.contains(":")) {
            throw new IllegalArgumentException("The path contains : which is not allowed");
        }
        
        String[] pathStrings = path.split("/");
        pathComponents = new ArrayList<String>();
        for (String currPathComponent : pathStrings) {
            if (currPathComponent != null && !currPathComponent.isEmpty()) {
                pathComponents.add(currPathComponent);
            }
        }
    }
    
    public Path(List<String> pathComponents) {
        this.pathComponents = pathComponents;
    }
    
    public List<String> getPathComponents() {
        return pathComponents;
    }

    /** Returns an iterator over the components of the path.

        <p>
        The iterator cannot be used to modify the path object - the
        <code>remove</code> method is not supported.

        @return The iterator.
     */
    @Override
    public Iterator<String> iterator()
    {
        return new PathIterator(this.pathComponents);
    }
    
    private class PathIterator implements Iterator<String> {

        private List<String> iteratorComponents;
        int position;
        
        public PathIterator(List<String> components) {
            this.iteratorComponents = components;
            position = 0;
        }
        
        @ Override
        public boolean hasNext() {            
            return (position != iteratorComponents.size());
        }

        @ Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            String nextComponent = iteratorComponents.get(position);
            position++;
            return nextComponent;
        }
        
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove path components while iterating");
        }
    }

    /** Lists the paths of all files in a directory tree on the local
        filesystem.

        @param directory The root directory of the directory tree.
        @return An array of relative paths, one for each file in the directory
                tree.
        @throws FileNotFoundException If the root directory does not exist.
        @throws IllegalArgumentException If <code>directory</code> exists but
                                         does not refer to a directory.
     */
    public static Path[] list(File directory) throws FileNotFoundException
    {
        if (directory == null) {
            throw new NullPointerException("Directory specified is null");
        } else if (!directory.exists()) {
            throw new FileNotFoundException("Directory specified does not exist");
        } else if (!directory.isDirectory()) {
            throw new IllegalArgumentException("Directory specified does not refer to a directory");
        }
        
        List<String> fileList = new ArrayList<String>();
        recursiveList(directory, "", fileList);
        int numFiles = fileList.size();
        int start = directory.getName().length() + 1;
        
        Path[] paths = new Path[numFiles];
        for (int i = 0; i < fileList.size(); i++) {
            paths[i] = new Path(fileList.get(i).substring(start));
        }
        
        return paths;
    }
    
    private static void recursiveList(File file, String filePath, List<String> fileList) {
        filePath += "/" + file.getName();

        if (file.isFile()) {
            fileList.add(filePath);
            return;
        }
        
        for (File currFile : file.listFiles()) {
            recursiveList(currFile, filePath, fileList);
        }
            
        return;
    }

    /** Determines whether the path represents the root directory.

        @return <code>true</code> if the path does represent the root directory,
                and <code>false</code> if it does not.
     */
    public boolean isRoot()
    {
        return (pathComponents.isEmpty());
    }

    /** Returns the path to the parent of this path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no parent.
     */
    public Path parent()
    {
        if (this.isRoot()) {
            throw new IllegalArgumentException("The root has no parent");
        }
        
        List<String> parentPath = new ArrayList<String>(this.pathComponents);
        parentPath.remove(parentPath.size() - 1);
        return (new Path(parentPath));
    }

    /** Returns the last component in the path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no last
                                         component.
     */
    public String last()
    {
        if (this.isRoot()) {
            throw new IllegalArgumentException("Path represents the root directory so no last component");
        }
        
        return pathComponents.get(pathComponents.size() - 1);
    }

    /** Determines if the given path is a subpath of this path.

        <p>
        The other path is a subpath of this path if is a prefix of this path.
        Note that by this definition, each path is a subpath of itself.

        @param other The path to be tested.
        @return <code>true</code> If and only if the other path is a subpath of
                this path.
     */
    public boolean isSubpath(Path other)
    {
        Iterator<String> otherIterator = other.iterator();
        Iterator<String> thisIterator = this.iterator();
        
        while (otherIterator.hasNext()) {
            if (!thisIterator.hasNext()) {
                return false;
            }
            
            if (!otherIterator.next().equals(thisIterator.next())) {
                return false;
            }
        }
        
        return true;
    }

    /** Converts the path to <code>File</code> object.

        @param root The resulting <code>File</code> object is created relative
                    to this directory.
        @return The <code>File</code> object.
     */
    public File toFile(File root)
    {
        return new File(root.getPath().concat(this.toString()));
    }

    /** Compares two paths for equality.

        <p>
        Two paths are equal if they share all the same components.

        @param other The other path.
        @return <code>true</code> if and only if the two paths are equal.
     */
    @Override
    public boolean equals(Object other)
    {
        return this.toString().equals(other.toString());
    }

    /** Returns the hash code of the path. */
    @Override
    public int hashCode()
    {
        return this.pathComponents.hashCode();
    }

    /** Converts the path to a string.

        <p>
        The string may later be used as an argument to the
        <code>Path(String)</code> constructor.

        @return The string representation of the path.
     */
    @Override
    public String toString()
    {
        StringBuilder path = new StringBuilder();
        path.append("/");
        for (String pathComponent : this.pathComponents) {
            path.append(pathComponent);
            path.append("/");
        }
        
        if (path.length() > 1) {
            path.deleteCharAt(path.length() - 1);
        }
        
        return path.toString();
    }


    /** Compares this path to another.

        <p>
        An ordering upon <code>Path</code> objects is provided to prevent
        deadlocks between applications that need to lock multiple filesystem
        objects simultaneously. By convention, paths that need to be locked
        simultaneously are locked in increasing order.

        <p>
        Because locking a path requires locking every component along the path,
        the order is not arbitrary. For example, suppose the paths were ordered
        first by length, so that <code>/etc</code> precedes
        <code>/bin/cat</code>, which precedes <code>/etc/dfs/conf.txt</code>.

        <p>
        Now, suppose two users are running two applications, such as two
        instances of <code>cp</code>. One needs to work with <code>/etc</code>
        and <code>/bin/cat</code>, and the other with <code>/bin/cat</code> and
        <code>/etc/dfs/conf.txt</code>.

        <p>
        Then, if both applications follow the convention and lock paths in
        increasing order, the following situation can occur: the first
        application locks <code>/etc</code>. The second application locks
        <code>/bin/cat</code>. The first application tries to lock
        <code>/bin/cat</code> also, but gets blocked because the second
        application holds the lock. Now, the second application tries to lock
        <code>/etc/dfs/conf.txt</code>, and also gets blocked, because it would
        need to acquire the lock for <code>/etc</code> to do so. The two
        applications are now deadlocked.

        @param other The other path.
        @return Zero if the two paths are equal, a negative number if this path
                precedes the other path, or a positive number if this path
                follows the other path.
     */
    @Override
    public int compareTo(Path other)
    {
        throw new UnsupportedOperationException("not implemented");
    }
    
}
