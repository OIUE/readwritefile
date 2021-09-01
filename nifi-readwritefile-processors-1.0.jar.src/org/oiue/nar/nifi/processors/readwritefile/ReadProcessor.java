/*     */ package org.oiue.nar.nifi.processors.readwritefile;
/*     */ 
/*     */ import java.io.BufferedReader;
/*     */ import java.io.File;
/*     */ import java.io.FileInputStream;
/*     */ import java.io.IOException;
/*     */ import java.io.InputStreamReader;
/*     */ import java.io.OutputStream;
/*     */ import java.nio.file.CopyOption;
/*     */ import java.nio.file.Files;
/*     */ import java.nio.file.Path;
/*     */ import java.nio.file.StandardCopyOption;
/*     */ import java.nio.file.attribute.FileAttribute;
/*     */ import java.util.ArrayList;
/*     */ import java.util.Collection;
/*     */ import java.util.HashMap;
/*     */ import java.util.HashSet;
/*     */ import java.util.List;
/*     */ import java.util.Map;
/*     */ import java.util.Set;
/*     */ import java.util.UUID;
/*     */ import java.util.concurrent.TimeUnit;
/*     */ import org.apache.commons.lang3.StringUtils;
/*     */ import org.apache.nifi.annotation.behavior.InputRequirement;
/*     */ import org.apache.nifi.annotation.behavior.ReadsAttribute;
/*     */ import org.apache.nifi.annotation.behavior.ReadsAttributes;
/*     */ import org.apache.nifi.annotation.behavior.Restricted;
/*     */ import org.apache.nifi.annotation.behavior.Restriction;
/*     */ import org.apache.nifi.annotation.behavior.WritesAttribute;
/*     */ import org.apache.nifi.annotation.behavior.WritesAttributes;
/*     */ import org.apache.nifi.annotation.documentation.CapabilityDescription;
/*     */ import org.apache.nifi.annotation.documentation.Tags;
/*     */ import org.apache.nifi.components.AllowableValue;
/*     */ import org.apache.nifi.components.PropertyDescriptor;
/*     */ import org.apache.nifi.components.RequiredPermission;
/*     */ import org.apache.nifi.components.ValidationContext;
/*     */ import org.apache.nifi.components.ValidationResult;
/*     */ import org.apache.nifi.expression.ExpressionLanguageScope;
/*     */ import org.apache.nifi.flowfile.FlowFile;
/*     */ import org.apache.nifi.flowfile.attributes.FragmentAttributes;
/*     */ import org.apache.nifi.logging.LogLevel;
/*     */ import org.apache.nifi.processor.AbstractProcessor;
/*     */ import org.apache.nifi.processor.ProcessContext;
/*     */ import org.apache.nifi.processor.ProcessSession;
/*     */ import org.apache.nifi.processor.Relationship;
/*     */ import org.apache.nifi.processor.exception.ProcessException;
/*     */ import org.apache.nifi.processor.util.StandardValidators;
/*     */ import org.apache.nifi.util.StopWatch;
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ @InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
/*     */ @Tags({"local", "files", "filesystem", "ingest", "ingress", "get", "source", "input", "fetch"})
/*     */ @CapabilityDescription("Reads the contents of a file from disk and streams it into the contents of an incoming FlowFile. Once this is done, the file is optionally moved elsewhere or deleted to help keep the file system organized.")
/*     */ @Restricted(restrictions = {@Restriction(requiredPermission = RequiredPermission.READ_FILESYSTEM, explanation = "Provides operator the ability to read from any file that NiFi has access to."), @Restriction(requiredPermission = RequiredPermission.WRITE_FILESYSTEM, explanation = "Provides operator the ability to delete any file that NiFi has access to.")})
/*     */ @ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
/*     */ @WritesAttributes({@WritesAttribute(attribute = "", description = "")})
/*     */ public class ReadProcessor
/*     */   extends AbstractProcessor
/*     */ {
/*  70 */   static final AllowableValue COMPLETION_NONE = new AllowableValue("None", "None", "Leave the file as-is");
/*  71 */   static final AllowableValue COMPLETION_MOVE = new AllowableValue("Move File", "Move File", "Moves the file to the directory specified by the <Move Destination Directory> property");
/*  72 */   static final AllowableValue COMPLETION_DELETE = new AllowableValue("Delete File", "Delete File", "Deletes the original file from the file system");
/*     */   
/*  74 */   static final AllowableValue CONFLICT_REPLACE = new AllowableValue("Replace File", "Replace File", "The newly ingested file should replace the existing file in the Destination Directory");
/*  75 */   static final AllowableValue CONFLICT_KEEP_INTACT = new AllowableValue("Keep Existing", "Keep Existing", "The existing file should in the Destination Directory should stay intact and the newly ingested file should be deleted");
/*     */   
/*  77 */   static final AllowableValue CONFLICT_FAIL = new AllowableValue("Fail", "Fail", "The existing destination file should remain intact and the incoming FlowFile should be routed to failure");
/*  78 */   static final AllowableValue CONFLICT_RENAME = new AllowableValue("Rename", "Rename", "The existing destination file should remain intact. The newly ingested file should be moved to the destination directory but be renamed to a random filename");
/*     */ 
/*     */   
/*  81 */   static final PropertyDescriptor FILENAME = (new PropertyDescriptor.Builder())
/*  82 */     .name("File to Fetch")
/*  83 */     .description("The fully-qualified filename of the file to fetch from the file system")
/*  84 */     .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
/*  85 */     .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
/*  86 */     .defaultValue("${absolute.path}/${filename}")
/*  87 */     .required(true)
/*  88 */     .build();
/*     */   
/*  90 */   static final PropertyDescriptor CHARSET = (new PropertyDescriptor.Builder())
/*  91 */     .name("character-set")
/*  92 */     .displayName("Character Set")
/*  93 */     .description("The Character Encoding that is used to encode/decode the file")
/*  94 */     .expressionLanguageSupported(ExpressionLanguageScope.NONE)
/*  95 */     .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
/*  96 */     .defaultValue("UTF-8")
/*  97 */     .required(true)
/*  98 */     .build();
/*     */   
/* 100 */   static final PropertyDescriptor IGNORE_HEADER = (new PropertyDescriptor.Builder())
/* 101 */     .name("ignore-header")
/* 102 */     .displayName("Ignore Header Rows")
/* 103 */     .description("If the first N lines are headers, will be ignored.")
/* 104 */     .expressionLanguageSupported(ExpressionLanguageScope.NONE)
/* 105 */     .required(true)
/* 106 */     .defaultValue("0")
/* 107 */     .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
/* 108 */     .build();
/*     */   
/* 110 */   static final PropertyDescriptor NROWS = (new PropertyDescriptor.Builder())
/* 111 */     .name("rows")
/* 112 */     .displayName("N of lines read)")
/* 113 */     .description("N rows read.")
/* 114 */     .expressionLanguageSupported(ExpressionLanguageScope.NONE)
/* 115 */     .required(true)
/* 116 */     .defaultValue("100")
/* 117 */     .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
/* 118 */     .build();
/*     */   
/* 120 */   static final PropertyDescriptor DELAY = (new PropertyDescriptor.Builder())
/* 121 */     .name("delay")
/* 122 */     .displayName("N of lines delay(milliseconds)")
/* 123 */     .description("Pause time per N rows read.")
/* 124 */     .expressionLanguageSupported(ExpressionLanguageScope.NONE)
/* 125 */     .required(true)
/* 126 */     .defaultValue("0")
/* 127 */     .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
/* 128 */     .build();
/*     */ 
/*     */   
/* 131 */   static final PropertyDescriptor COMPLETION_STRATEGY = (new PropertyDescriptor.Builder())
/* 132 */     .name("Completion Strategy")
/* 133 */     .description("Specifies what to do with the original file on the file system once it has been pulled into NiFi")
/* 134 */     .expressionLanguageSupported(ExpressionLanguageScope.NONE)
/* 135 */     .allowableValues(new AllowableValue[] { COMPLETION_NONE, COMPLETION_MOVE, COMPLETION_DELETE
/* 136 */       }).defaultValue(COMPLETION_NONE.getValue())
/* 137 */     .required(true)
/* 138 */     .build();
/*     */   
/* 140 */   static final PropertyDescriptor MOVE_DESTINATION_DIR = (new PropertyDescriptor.Builder())
/* 141 */     .name("Move Destination Directory")
/* 142 */     .description("The directory to the move the original file to once it has been fetched from the file system. This property is ignored unless the Completion Strategy is set to \"Move File\". If the directory does not exist, it will be created.")
/*     */     
/* 144 */     .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
/* 145 */     .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
/* 146 */     .required(false)
/* 147 */     .build();
/*     */   
/* 149 */   static final PropertyDescriptor CONFLICT_STRATEGY = (new PropertyDescriptor.Builder())
/* 150 */     .name("Move Conflict Strategy")
/* 151 */     .description("If Completion Strategy is set to Move File and a file already exists in the destination directory with the same name, this property specifies how that naming conflict should be resolved")
/*     */     
/* 153 */     .allowableValues(new AllowableValue[] { CONFLICT_RENAME, CONFLICT_REPLACE, CONFLICT_KEEP_INTACT, CONFLICT_FAIL
/* 154 */       }).defaultValue(CONFLICT_RENAME.getValue())
/* 155 */     .required(true)
/* 156 */     .build();
/*     */   
/* 158 */   static final PropertyDescriptor FILE_NOT_FOUND_LOG_LEVEL = (new PropertyDescriptor.Builder())
/* 159 */     .name("Log level when file not found")
/* 160 */     .description("Log level to use in case the file does not exist when the processor is triggered")
/* 161 */     .allowableValues((Enum[])LogLevel.values())
/* 162 */     .defaultValue(LogLevel.ERROR.toString())
/* 163 */     .required(true)
/* 164 */     .build();
/*     */   
/* 166 */   static final PropertyDescriptor PERM_DENIED_LOG_LEVEL = (new PropertyDescriptor.Builder())
/* 167 */     .name("Log level when permission denied")
/* 168 */     .description("Log level to use in case user " + System.getProperty("user.name") + " does not have sufficient permissions to read the file")
/* 169 */     .allowableValues((Enum[])LogLevel.values())
/* 170 */     .defaultValue(LogLevel.ERROR.toString())
/* 171 */     .required(true)
/* 172 */     .build();
/*     */   
/* 174 */   public static final Relationship REL_ORIGINAL = (new Relationship.Builder())
/* 175 */     .name("original")
/* 176 */     .description("The original file")
/* 177 */     .build();
/* 178 */   static final Relationship REL_PACKET = (new Relationship.Builder())
/* 179 */     .name("Packet data")
/* 180 */     .description("Any FlowFile that is success read a line from the file system will be transferred to this Relationship.")
/* 181 */     .build();
/* 182 */   static final Relationship REL_SUCCESS = (new Relationship.Builder())
/* 183 */     .name("success")
/* 184 */     .description("Any FlowFile that is successfully read from the file system will be transferred to this Relationship.")
/* 185 */     .build();
/* 186 */   static final Relationship REL_NOT_FOUND = (new Relationship.Builder())
/* 187 */     .name("not.found")
/* 188 */     .description("Any FlowFile that could not be fetched from the file system because the file could not be found will be transferred to this Relationship.")
/* 189 */     .build();
/* 190 */   static final Relationship REL_PERMISSION_DENIED = (new Relationship.Builder())
/* 191 */     .name("permission.denied")
/* 192 */     .description("Any FlowFile that could not be fetched from the file system due to the user running NiFi not having sufficient permissions will be transferred to this Relationship.")
/* 193 */     .build();
/* 194 */   static final Relationship REL_FAILURE = (new Relationship.Builder())
/* 195 */     .name("failure")
/* 196 */     .description("Any FlowFile that could not be fetched from the file system for any reason other than insufficient permissions or the file not existing will be transferred to this Relationship.")
/*     */     
/* 198 */     .build();
/*     */ 
/*     */   
/*     */   protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
/* 202 */     List<PropertyDescriptor> properties = new ArrayList<>();
/* 203 */     properties.add(FILENAME);
/* 204 */     properties.add(CHARSET);
/* 205 */     properties.add(IGNORE_HEADER);
/* 206 */     properties.add(NROWS);
/* 207 */     properties.add(DELAY);
/* 208 */     properties.add(COMPLETION_STRATEGY);
/* 209 */     properties.add(MOVE_DESTINATION_DIR);
/* 210 */     properties.add(CONFLICT_STRATEGY);
/* 211 */     properties.add(FILE_NOT_FOUND_LOG_LEVEL);
/* 212 */     properties.add(PERM_DENIED_LOG_LEVEL);
/* 213 */     return properties;
/*     */   }
/*     */ 
/*     */   
/*     */   public Set<Relationship> getRelationships() {
/* 218 */     Set<Relationship> relationships = new HashSet<>();
/* 219 */     relationships.add(REL_ORIGINAL);
/* 220 */     relationships.add(REL_PACKET);
/* 221 */     relationships.add(REL_SUCCESS);
/* 222 */     relationships.add(REL_NOT_FOUND);
/* 223 */     relationships.add(REL_PERMISSION_DENIED);
/* 224 */     relationships.add(REL_FAILURE);
/* 225 */     return relationships;
/*     */   }
/*     */ 
/*     */   
/*     */   protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
/* 230 */     List<ValidationResult> results = new ArrayList<>();
/*     */     
/* 232 */     if (COMPLETION_MOVE.getValue().equalsIgnoreCase(validationContext.getProperty(COMPLETION_STRATEGY).getValue()) && 
/* 233 */       !validationContext.getProperty(MOVE_DESTINATION_DIR).isSet()) {
/* 234 */       results.add((new ValidationResult.Builder()).subject(MOVE_DESTINATION_DIR.getName()).input(null).valid(false).explanation(MOVE_DESTINATION_DIR
/* 235 */             .getName() + " must be specified if " + COMPLETION_STRATEGY.getName() + " is set to " + COMPLETION_MOVE.getDisplayName()).build());
/*     */     }
/*     */ 
/*     */     
/* 239 */     return results;
/*     */   }
/*     */ 
/*     */   
/*     */   public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
/* 244 */     FlowFile flowFile = session.get();
/* 245 */     if (flowFile == null) {
/*     */       return;
/*     */     }
/*     */     
/* 249 */     StopWatch stopWatch = new StopWatch(true);
/* 250 */     String filename = context.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();
/* 251 */     String charSet = context.getProperty(CHARSET).getValue();
/* 252 */     LogLevel levelFileNotFound = LogLevel.valueOf(context.getProperty(FILE_NOT_FOUND_LOG_LEVEL).getValue());
/* 253 */     LogLevel levelPermDenied = LogLevel.valueOf(context.getProperty(PERM_DENIED_LOG_LEVEL).getValue());
/* 254 */     File file = new File(filename);
/* 255 */     int ignore_header = context.getProperty(IGNORE_HEADER).asInteger().intValue();
/* 256 */     int _delay = context.getProperty(DELAY).asInteger().intValue();
/* 257 */     int _nrows = context.getProperty(NROWS).asInteger().intValue();
/*     */ 
/*     */     
/* 260 */     Path filePath = file.toPath();
/* 261 */     if (!Files.exists(filePath, new java.nio.file.LinkOption[0]) && !Files.notExists(filePath, new java.nio.file.LinkOption[0])) {
/* 262 */       getLogger().log(levelFileNotFound, "Could not fetch file {} from file system for {} because the existence of the file cannot be verified; routing to failure", new Object[] { file, flowFile });
/*     */       
/* 264 */       session.transfer(session.penalize(flowFile), REL_FAILURE); return;
/*     */     } 
/* 266 */     if (!Files.exists(filePath, new java.nio.file.LinkOption[0])) {
/* 267 */       getLogger().log(levelFileNotFound, "Could not fetch file {} from file system for {} because the file does not exist; routing to not.found", new Object[] { file, flowFile });
/* 268 */       session.getProvenanceReporter().route(flowFile, REL_NOT_FOUND);
/* 269 */       session.transfer(session.penalize(flowFile), REL_NOT_FOUND);
/*     */       
/*     */       return;
/*     */     } 
/*     */     
/* 274 */     String user = System.getProperty("user.name");
/* 275 */     if (!isReadable(file)) {
/* 276 */       getLogger().log(levelPermDenied, "Could not fetch file {} from file system for {} due to user {} not having sufficient permissions to read the file; routing to permission.denied", new Object[] { file, flowFile, user });
/*     */       
/* 278 */       session.getProvenanceReporter().route(flowFile, REL_PERMISSION_DENIED);
/* 279 */       session.transfer(session.penalize(flowFile), REL_PERMISSION_DENIED);
/*     */ 
/*     */       
/*     */       return;
/*     */     } 
/*     */     
/* 285 */     String completionStrategy = context.getProperty(COMPLETION_STRATEGY).getValue();
/* 286 */     String targetDirectoryName = context.getProperty(MOVE_DESTINATION_DIR).evaluateAttributeExpressions(flowFile).getValue();
/* 287 */     if (targetDirectoryName != null) {
/* 288 */       File targetDir = new File(targetDirectoryName);
/* 289 */       if (COMPLETION_MOVE.getValue().equalsIgnoreCase(completionStrategy)) {
/* 290 */         if (targetDir.exists() && (!isWritable(targetDir) || !isDirectory(targetDir))) {
/* 291 */           getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, but that is not a directory or user {} does not have permissions to write to that directory", new Object[] { file, flowFile, targetDir, user });
/*     */ 
/*     */           
/* 294 */           session.transfer(flowFile, REL_FAILURE);
/*     */           
/*     */           return;
/*     */         } 
/* 298 */         if (!targetDir.exists()) {
/*     */           try {
/* 300 */             Files.createDirectories(targetDir.toPath(), (FileAttribute<?>[])new FileAttribute[0]);
/* 301 */           } catch (Exception e) {
/* 302 */             getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, but that directory does not exist and could not be created due to: {}", new Object[] { file, flowFile, targetDir, e
/*     */                   
/* 304 */                   .getMessage() }, e);
/* 305 */             session.transfer(flowFile, REL_FAILURE);
/*     */             
/*     */             return;
/*     */           } 
/*     */         }
/* 310 */         String conflictStrategy = context.getProperty(CONFLICT_STRATEGY).getValue();
/*     */         
/* 312 */         if (CONFLICT_FAIL.getValue().equalsIgnoreCase(conflictStrategy)) {
/* 313 */           File targetFile = new File(targetDir, file.getName());
/* 314 */           if (targetFile.exists()) {
/* 315 */             getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, but a file with name {} already exists in that directory and the Move Conflict Strategy is configured for failure", new Object[] { file, flowFile, targetDir, file
/*     */                   
/* 317 */                   .getName() });
/* 318 */             session.transfer(flowFile, REL_FAILURE);
/*     */             return;
/*     */           } 
/*     */         } 
/*     */       } 
/*     */     } 
/* 324 */     Map<String, String> attributes = new HashMap<>();
/* 325 */     attributes.putAll(flowFile.getAttributes());
/*     */     
/* 327 */     session.getProvenanceReporter().fetch(flowFile, file.toURI().toString(), "Replaced content of FlowFile with contents of " + file.toURI(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
/* 328 */     session.transfer(flowFile, REL_ORIGINAL);
/*     */     
/* 330 */     String fragmentId = UUID.randomUUID().toString();
/* 331 */     BufferedReader br = null;
/* 332 */     long i = 0L;
/* 333 */     try (FileInputStream fis = new FileInputStream(file)) {
/* 334 */       br = new BufferedReader(new InputStreamReader(fis, charSet));
/* 335 */       String line = null;
/* 336 */       while ((line = br.readLine()) != null)
/*     */       {
/* 338 */         if (ignore_header > 0) { ignore_header--; continue; }
/* 339 */          FlowFile packet = session.create();
/* 340 */         String finalLine = line;
/* 341 */         packet = session.write(packet, out -> out.write(finalLine.getBytes()));
/*     */ 
/*     */ 
/*     */ 
/*     */         
/* 346 */         attributes.put(FragmentAttributes.FRAGMENT_ID.key(), fragmentId);
/* 347 */         attributes.put(FragmentAttributes.FRAGMENT_INDEX.key(), Long.toString(i));
/* 348 */         packet = session.putAllAttributes(packet, attributes);
/* 349 */         session.getProvenanceReporter().create(packet);
/* 350 */         session.transfer(packet, REL_PACKET);
/* 351 */         session.commit();
/* 352 */         i++;
/*     */         
/*     */         try {
/* 355 */           if (_delay > 0 && i % _nrows == 0L) {
/* 356 */             Thread.sleep(_delay);
/*     */           }
/* 358 */         } catch (InterruptedException e) {
/* 359 */           getLogger().error(e.getMessage(), e);
/*     */         }
/*     */       
/*     */       }
/*     */     
/* 364 */     } catch (IOException ioe) {
/* 365 */       getLogger().error("Could not fetch file {} from file system for {} due to {}; routing to failure", new Object[] { file, flowFile, ioe.toString() }, ioe);
/* 366 */       session.transfer(session.penalize(flowFile), REL_FAILURE);
/*     */       return;
/*     */     } finally {
/* 369 */       if (br != null) {
/*     */         try {
/* 371 */           br.close();
/* 372 */         } catch (IOException e) {
/* 373 */           getLogger().error(e.getMessage(), e);
/*     */         } 
/*     */       }
/*     */     } 
/*     */     
/* 378 */     attributes.put(FragmentAttributes.FRAGMENT_ID.key(), fragmentId);
/* 379 */     attributes.remove(FragmentAttributes.FRAGMENT_INDEX.key());
/* 380 */     attributes.put(FragmentAttributes.FRAGMENT_COUNT.key(), Long.toString(i));
/* 381 */     flowFile = session.create();
/* 382 */     session.getProvenanceReporter().create(flowFile);
/* 383 */     session.transfer(session.putAllAttributes(flowFile, attributes), REL_SUCCESS);
/*     */ 
/*     */ 
/*     */ 
/*     */     
/* 388 */     session.commit();
/*     */ 
/*     */     
/* 391 */     Exception completionFailureException = null;
/* 392 */     if (COMPLETION_DELETE.getValue().equalsIgnoreCase(completionStrategy)) {
/*     */       
/*     */       try {
/* 395 */         delete(file);
/* 396 */       } catch (IOException ioe) {
/* 397 */         completionFailureException = ioe;
/*     */       } 
/* 399 */     } else if (COMPLETION_MOVE.getValue().equalsIgnoreCase(completionStrategy)) {
/* 400 */       File targetDirectory = new File(targetDirectoryName);
/* 401 */       File targetFile = new File(targetDirectory, file.getName());
/*     */       try {
/* 403 */         if (targetFile.exists()) {
/* 404 */           String conflictStrategy = context.getProperty(CONFLICT_STRATEGY).getValue();
/* 405 */           if (CONFLICT_KEEP_INTACT.getValue().equalsIgnoreCase(conflictStrategy)) {
/*     */             
/* 407 */             Files.delete(file.toPath());
/* 408 */           } else if (CONFLICT_RENAME.getValue().equalsIgnoreCase(conflictStrategy)) {
/*     */             
/* 410 */             String newName, simpleFilename = targetFile.getName();
/*     */             
/* 412 */             if (simpleFilename.contains(".")) {
/* 413 */               newName = StringUtils.substringBeforeLast(simpleFilename, ".") + "-" + UUID.randomUUID().toString() + "." + StringUtils.substringAfterLast(simpleFilename, ".");
/*     */             } else {
/* 415 */               newName = simpleFilename + "-" + UUID.randomUUID().toString();
/*     */             } 
/*     */             
/* 418 */             move(file, new File(targetDirectory, newName), false);
/* 419 */           } else if (CONFLICT_REPLACE.getValue().equalsIgnoreCase(conflictStrategy)) {
/* 420 */             move(file, targetFile, true);
/*     */           } 
/*     */         } else {
/* 423 */           move(file, targetFile, false);
/*     */         } 
/* 425 */       } catch (IOException ioe) {
/* 426 */         completionFailureException = ioe;
/*     */       } 
/*     */     } 
/*     */ 
/*     */     
/* 431 */     if (completionFailureException != null) {
/* 432 */       getLogger().warn("Successfully fetched the content from {} for {} but failed to perform Completion Action due to {}; routing to success", new Object[] { file, flowFile, completionFailureException }, completionFailureException);
/*     */     }
/*     */   }
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */   
/*     */   protected void move(File source, File target, boolean overwrite) throws IOException {
/* 442 */     File targetDirectory = target.getParentFile();
/*     */ 
/*     */     
/* 445 */     Path targetPath = target.toPath();
/* 446 */     if (!targetDirectory.exists()) {
/* 447 */       Files.createDirectories(targetDirectory.toPath(), (FileAttribute<?>[])new FileAttribute[0]);
/*     */     }
/*     */     
/* 450 */     (new CopyOption[1])[0] = StandardCopyOption.REPLACE_EXISTING; CopyOption[] copyOptions = overwrite ? new CopyOption[1] : new CopyOption[0];
/* 451 */     Files.move(source.toPath(), targetPath, copyOptions);
/*     */   }
/*     */   
/*     */   protected void delete(File file) throws IOException {
/* 455 */     Files.delete(file.toPath());
/*     */   }
/*     */   
/*     */   protected boolean isReadable(File file) {
/* 459 */     return file.canRead();
/*     */   }
/*     */   
/*     */   protected boolean isWritable(File file) {
/* 463 */     return file.canWrite();
/*     */   }
/*     */   
/*     */   protected boolean isDirectory(File file) {
/* 467 */     return file.isDirectory();
/*     */   }
/*     */ }


/* Location:              /Users/every/docker/gf/extensions/广发数据批处理流程v1.0.5_提交版/META-INF/bundled-dependencies/nifi-readwritefile-processors-1.0.jar!/org/oiue/nar/nifi/processors/readwritefile/ReadProcessor.class
 * Java compiler version: 8 (52.0)
 * JD-Core Version:       1.1.3
 */