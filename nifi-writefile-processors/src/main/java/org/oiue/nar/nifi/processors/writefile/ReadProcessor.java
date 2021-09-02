package org.oiue.nar.nifi.processors.writefile;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.*;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.oiue.tools.string.StringUtil;

import java.io.*;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileAttribute;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"local", "files", "filesystem", "ingest", "ingress", "get", "source", "input", "fetch"})
@CapabilityDescription("Reads the contents of a file from disk and streams it into the contents of an incoming FlowFile. Once this is done, the file is optionally moved elsewhere or deleted to help keep the file system organized.")
@Restricted(restrictions = {@Restriction(requiredPermission = RequiredPermission.READ_FILESYSTEM, explanation = "Provides operator the ability to read from any file that NiFi has access to."), @Restriction(requiredPermission = RequiredPermission.WRITE_FILESYSTEM, explanation = "Provides operator the ability to delete any file that NiFi has access to.")})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class ReadProcessor extends AbstractProcessor {
    public static final Map<String,Map> catchfile = new ConcurrentHashMap();
    public static final AllowableValue COMPLETION_NONE = new AllowableValue("None", "None", "Leave the file as-is");
    public static final AllowableValue COMPLETION_MOVE = new AllowableValue("Move File", "Move File", "Moves the file to the directory specified by the <Move Destination Directory> property");
    public static final AllowableValue COMPLETION_DELETE = new AllowableValue("Delete File", "Delete File", "Deletes the original file from the file system");
    public static final AllowableValue CONFLICT_REPLACE = new AllowableValue("Replace File", "Replace File", "The newly ingested file should replace the existing file in the Destination Directory");
    public static final AllowableValue CONFLICT_KEEP_INTACT = new AllowableValue("Keep Existing", "Keep Existing", "The existing file should in the Destination Directory should stay intact and the newly ingested file should be deleted");
    public static final AllowableValue CONFLICT_FAIL = new AllowableValue("Fail", "Fail", "The existing destination file should remain intact and the incoming FlowFile should be routed to failure");
    public static final AllowableValue CONFLICT_RENAME = new AllowableValue("Rename", "Rename", "The existing destination file should remain intact. The newly ingested file should be moved to the destination directory but be renamed to a random filename");

    public static final PropertyDescriptor FILENAME = (new PropertyDescriptor.Builder())
            .name("File to Fetch")
            .description("The fully-qualified filename of the file to fetch from the file system")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${absolute.path}/${filename}")
            .required(true)
            .build();
    public static final PropertyDescriptor CHARSET = (new PropertyDescriptor.Builder())
            .name("character-set")
            .displayName("Character Set")
            .description("The Character Encoding that is used to encode/decode the file")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .required(true)
            .build();
    public static final PropertyDescriptor IGNORE_HEADER = (new PropertyDescriptor.Builder())
            .name("ignore-header")
            .displayName("Ignore Header Rows")
            .description("If the first N lines are headers, will be ignored.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor NROWS = (new PropertyDescriptor.Builder())
            .name("rows")
            .displayName("N of lines read)")
            .description("N rows read.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor DELAY = (new PropertyDescriptor.Builder())
            .name("delay")
            .displayName("N of lines delay(milliseconds)")
            .description("Pause time per N rows read.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor COMPLETION_STRATEGY = (new PropertyDescriptor.Builder())
            .name("Completion Strategy")
            .description("Specifies what to do with the original file on the file system once it has been pulled into NiFi")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(new AllowableValue[]{COMPLETION_NONE, COMPLETION_MOVE, COMPLETION_DELETE})
            .defaultValue(COMPLETION_NONE.getValue())
            .required(true)
            .build();
    public static final PropertyDescriptor MOVE_DESTINATION_DIR = (new PropertyDescriptor.Builder())
            .name("Move Destination Directory")
            .description("The directory to the move the original file to once it has been fetched from the file system. This property is ignored unless the Completion Strategy is set to \"Move File\". If the directory does not exist, it will be created.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor CONFLICT_STRATEGY = (new PropertyDescriptor.Builder())
            .name("Move Conflict Strategy")
            .description("If Completion Strategy is set to Move File and a file already exists in the destination directory with the same name, this property specifies how that naming conflict should be resolved")
            .allowableValues(new AllowableValue[]{CONFLICT_RENAME, CONFLICT_REPLACE, CONFLICT_KEEP_INTACT, CONFLICT_FAIL})
            .defaultValue(CONFLICT_RENAME.getValue())
            .required(true)
            .build();
    public static final PropertyDescriptor FILE_NOT_FOUND_LOG_LEVEL = (new PropertyDescriptor.Builder())
            .name("Log level when file not found")
            .description("Log level to use in case the file does not exist when the processor is triggered")
            .allowableValues((Enum[]) LogLevel.values())
            .defaultValue(LogLevel.ERROR.toString())
            .required(true)
            .build();
    public static final PropertyDescriptor PERM_DENIED_LOG_LEVEL = (new PropertyDescriptor.Builder())
            .name("Log level when permission denied")
            .description("Log level to use in case user " + System.getProperty("user.name") + " does not have sufficient permissions to read the file")
            .allowableValues((Enum[]) LogLevel.values())
            .defaultValue(LogLevel.ERROR.toString())
            .required(true)
            .build();

    public static final Relationship REL_ORIGINAL = (new Relationship.Builder())
            .name("original")
            .description("The original file")
            .build();
    public static final Relationship REL_FRAGMENT = (new Relationship.Builder())
            .name("File fragment")
            .description("File fragment flowFile that is success read N lines from the file system will be transferred to this Relationship.")
            .build();
    public static final Relationship REL_HAS_NEXT = (new Relationship.Builder())
            .name("File has next fragment")
            .description("There is the next fragment of the file will be transferred to this Relationship.")
            .build();
    public static final Relationship REL_SUCCESS = (new Relationship.Builder())
            .name("success")
            .description("Any FlowFile that is successfully read from the file system will be transferred to this Relationship.")
            .build();
    public static final Relationship REL_NOT_FOUND = (new Relationship.Builder())
            .name("not.found")
            .description("Any FlowFile that could not be fetched from the file system because the file could not be found will be transferred to this Relationship.")
            .build();
    public static final Relationship REL_PERMISSION_DENIED = (new Relationship.Builder())
            .name("permission.denied")
            .description("Any FlowFile that could not be fetched from the file system due to the user running NiFi not having sufficient permissions will be transferred to this Relationship.")
            .build();
    public static final Relationship REL_FAILURE = (new Relationship.Builder())
            .name("failure")
            .description("Any FlowFile that could not be fetched from the file system for any reason other than insufficient permissions or the file not existing will be transferred to this Relationship.")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(FILENAME);
        properties.add(CHARSET);
        properties.add(IGNORE_HEADER);
        properties.add(NROWS);
        properties.add(DELAY);
        properties.add(COMPLETION_STRATEGY);
        properties.add(MOVE_DESTINATION_DIR);
        properties.add(CONFLICT_STRATEGY);
        properties.add(FILE_NOT_FOUND_LOG_LEVEL);
        properties.add(PERM_DENIED_LOG_LEVEL);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FRAGMENT);
        relationships.add(REL_HAS_NEXT);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_NOT_FOUND);
        relationships.add(REL_PERMISSION_DENIED);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>();
        if (COMPLETION_MOVE.getValue()
                .equalsIgnoreCase(validationContext.getProperty(COMPLETION_STRATEGY).getValue()) && !validationContext.getProperty(MOVE_DESTINATION_DIR).isSet()) {
            results.add((new ValidationResult.Builder())
                    .subject(MOVE_DESTINATION_DIR.getName())
                    .input(null)
                    .valid(false)
                    .explanation(MOVE_DESTINATION_DIR.getName() + " must be specified if " + COMPLETION_STRATEGY.getName() + " is set to " + COMPLETION_MOVE.getDisplayName())
                    .build());
        }
        return results;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        Map fileinfo = null;
        int _nrows = context.getProperty(NROWS).asInteger().intValue();
        int ignore_header = 0;
        long i = 1L;
        long r = 1L;
        File file = null;
        BufferedReader br = null;

        String charSet = context.getProperty(CHARSET).getValue();
        String completionStrategy = context.getProperty(COMPLETION_STRATEGY).getValue();
        String targetDirectoryName = context.getProperty(MOVE_DESTINATION_DIR).evaluateAttributeExpressions(flowFile).getValue();

        Map<String, String> attributes = new HashMap<>();
        attributes.putAll(flowFile.getAttributes());
        String fragmentId = attributes.get(FragmentAttributes.FRAGMENT_ID.key());
        if(StringUtil.isEmptys(fragmentId)){
            StopWatch stopWatch = new StopWatch(true);
            String filename = context.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();
            LogLevel levelFileNotFound = LogLevel.valueOf(context.getProperty(FILE_NOT_FOUND_LOG_LEVEL).getValue());
            LogLevel levelPermDenied = LogLevel.valueOf(context.getProperty(PERM_DENIED_LOG_LEVEL).getValue());
            file = new File(filename);
            ignore_header = context.getProperty(IGNORE_HEADER).asInteger().intValue();
            Path filePath = file.toPath();
            if (!Files.exists(filePath, new java.nio.file.LinkOption[0]) && !Files.notExists(filePath, new java.nio.file.LinkOption[0])) {
                getLogger().log(levelFileNotFound, "Could not fetch file {} from file system for {} because the existence of the file cannot be verified; routing to failure", new Object[]{file, flowFile});
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }
            if (!Files.exists(filePath, new java.nio.file.LinkOption[0])) {
                getLogger().log(levelFileNotFound, "Could not fetch file {} from file system for {} because the file does not exist; routing to not.found", new Object[]{file, flowFile});
                session.getProvenanceReporter().route(flowFile, REL_NOT_FOUND);
                session.transfer(session.penalize(flowFile), REL_NOT_FOUND);
                return;
            }
            String user = System.getProperty("user.name");
            if (!isReadable(file)) {
                getLogger().log(levelPermDenied, "Could not fetch file {} from file system for {} due to user {} not having sufficient permissions to read the file; routing to permission.denied", new Object[]{file, flowFile, user});
                session.getProvenanceReporter().route(flowFile, REL_PERMISSION_DENIED);
                session.transfer(session.penalize(flowFile), REL_PERMISSION_DENIED);
                return;
            }
            if (targetDirectoryName != null) {
                File targetDir = new File(targetDirectoryName);
                if (COMPLETION_MOVE.getValue().equalsIgnoreCase(completionStrategy)) {
                    if (targetDir.exists() && (!isWritable(targetDir) || !isDirectory(targetDir))) {
                        getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, but that is not a directory or user {} does not have permissions to write to that directory", new Object[]{file, flowFile, targetDir, user});
                        session.transfer(flowFile, REL_FAILURE);
                        return;
                    }
                    if (!targetDir.exists()) {
                        try {
                            Files.createDirectories(targetDir.toPath(), (FileAttribute<?>[]) new FileAttribute[0]);
                        } catch (Exception e) {
                            getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, but that directory does not exist and could not be created due to: {}", new Object[]{file, flowFile, targetDir, e.getMessage()}, e);
                            session.transfer(flowFile, REL_FAILURE);
                            return;
                        }
                    }
                    String conflictStrategy = context.getProperty(CONFLICT_STRATEGY).getValue();
                    if (CONFLICT_FAIL.getValue().equalsIgnoreCase(conflictStrategy)) {
                        File targetFile = new File(targetDir, file.getName());
                        if (targetFile.exists()) {
                            getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, but a file with name {} already exists in that directory and the Move Conflict Strategy is configured for failure", new Object[]{file, flowFile, targetDir, file.getName()});
                            session.transfer(flowFile, REL_FAILURE);
                            return;
                        }
                    }
                }
            }
            session.getProvenanceReporter().fetch(flowFile, file.toURI().toString(), "Replaced content of FlowFile with contents of " + file.toURI(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_ORIGINAL);
            session.commit();
            fragmentId = attributes.get("uuid");
        }else{
            getLogger().debug("catchfile:"+catchfile);
            fileinfo = catchfile.remove(fragmentId);
            file = (File) fileinfo.get("file");
            br = (BufferedReader) fileinfo.get("br");
            i = (long) fileinfo.get("index");
            r = (long) fileinfo.get("rows");
        }
        boolean over = true;
        try {
            if(br==null) {
                br = new BufferedReader(new InputStreamReader(new FileInputStream(file), charSet));
            }
            String line = null;
            StringBuffer rows = new StringBuffer();
            while ((line = br.readLine()) != null) {
                if (ignore_header > 0) {
                    ignore_header--;
                    continue;
                }
                r++;
                rows.append(line).append("\n");
                if (_nrows!=-1){
                    if(--_nrows==0){
                        over = false;
                        break;
                    }
                }
            }

            FlowFile packet = session.create();
            packet = session.write(packet, out -> out.write(rows.toString().getBytes()));
            attributes.put(FragmentAttributes.FRAGMENT_ID.key(), fragmentId);
            attributes.put(FragmentAttributes.FRAGMENT_INDEX.key(), Long.toString(i));
            session.transfer(session.putAllAttributes(packet, attributes), REL_FRAGMENT);
            session.commit();

            if(!over) {
                if (fileinfo == null){
                    fileinfo = new HashMap();
                }
                fileinfo.put("file",file);
                fileinfo.put("br",br);
                fileinfo.put("index",++i);
                fileinfo.put("rows",r);
                catchfile.put(fragmentId,fileinfo);

                FlowFile has_packet = session.create();
                has_packet = session.write(has_packet, out -> out.write(rows.toString().getBytes()));
                attributes.put(FragmentAttributes.FRAGMENT_ID.key(), fragmentId);
                attributes.put(FragmentAttributes.FRAGMENT_INDEX.key(), Long.toString(i));
                has_packet = session.putAllAttributes(has_packet, attributes);
                session.getProvenanceReporter().clone(flowFile,has_packet);
                session.transfer(has_packet, REL_HAS_NEXT);
                return;
            }
        } catch (IOException ioe) {
            getLogger().error("Could not fetch file {} from file system for {} due to {}; routing to failure", new Object[]{file, flowFile, ioe.toString()}, ioe);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        } catch (Throwable ioe) {
            getLogger().error("msg:{}",new Object[]{ioe.getMessage()},ioe);
        } finally {
            if (br != null&&over) {
                try {
                    br.close();
                } catch (IOException e) {
                    getLogger().error(e.getMessage(), e);
                }
            }
        }

        attributes.put(FragmentAttributes.FRAGMENT_ID.key(), fragmentId);
        attributes.remove(FragmentAttributes.FRAGMENT_INDEX.key());
        attributes.put(FragmentAttributes.FRAGMENT_COUNT.key(), Long.toString(i));
        flowFile = session.create();
        session.transfer(session.putAllAttributes(flowFile, attributes), REL_SUCCESS);
        session.commit();

        Exception completionFailureException = null;
        if (COMPLETION_DELETE.getValue().equalsIgnoreCase(completionStrategy)) {
            try {
                delete(file);
            } catch (IOException ioe) {
                completionFailureException = ioe;
            }
        } else if (COMPLETION_MOVE.getValue().equalsIgnoreCase(completionStrategy)) {
            File targetDirectory = new File(targetDirectoryName);
            File targetFile = new File(targetDirectory, file.getName());
            try {
                if (targetFile.exists()) {
                    String conflictStrategy = context.getProperty(CONFLICT_STRATEGY).getValue();
                    if (CONFLICT_KEEP_INTACT.getValue().equalsIgnoreCase(conflictStrategy)) {
                        Files.delete(file.toPath());
                    } else if (CONFLICT_RENAME.getValue().equalsIgnoreCase(conflictStrategy)) {
                        String newName, simpleFilename = targetFile.getName();
                        if (simpleFilename.contains(".")) {
                            newName = StringUtils.substringBeforeLast(simpleFilename, ".") + "-" + UUID.randomUUID()
                                    .toString() + "." + StringUtils.substringAfterLast(simpleFilename, ".");
                        } else {
                            newName = simpleFilename + "-" + UUID.randomUUID().toString();
                        }
                        move(file, new File(targetDirectory, newName), false);
                    } else if (CONFLICT_REPLACE.getValue().equalsIgnoreCase(conflictStrategy)) {
                        move(file, targetFile, true);
                    }
                } else {
                    move(file, targetFile, false);
                }
            } catch (IOException ioe) {
                completionFailureException = ioe;
            }
        }
        if (completionFailureException != null) {
            getLogger().warn("Successfully fetched the content from {} for {} but failed to perform Completion Action due to {}; routing to success", new Object[]{file, flowFile, completionFailureException}, completionFailureException);
        }
    }

    protected void move(File source, File target, boolean overwrite) throws IOException {
        File targetDirectory = target.getParentFile();
        Path targetPath = target.toPath();
        if (!targetDirectory.exists()) {
            Files.createDirectories(targetDirectory.toPath(), (FileAttribute<?>[]) new FileAttribute[0]);
        }
        (new CopyOption[1])[0] = StandardCopyOption.REPLACE_EXISTING;
        CopyOption[] copyOptions = overwrite ? new CopyOption[1] : new CopyOption[0];
        Files.move(source.toPath(), targetPath, copyOptions);
    }

    protected void delete(File file) throws IOException {
        Files.delete(file.toPath());
    }

    protected boolean isReadable(File file) {
        return file.canRead();
    }

    protected boolean isWritable(File file) {
        return file.canWrite();
    }

    protected boolean isDirectory(File file) {
        return file.isDirectory();
    }
}
