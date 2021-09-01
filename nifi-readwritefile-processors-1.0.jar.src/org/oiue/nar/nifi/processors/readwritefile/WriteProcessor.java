/*     */ package org.oiue.nar.nifi.processors.readwritefile;
/*     */ import java.nio.file.Files;
/*     */ import java.nio.file.Path;
/*     */ import java.nio.file.Paths;
/*     */ import java.nio.file.attribute.FileAttribute;
/*     */ import java.nio.file.attribute.PosixFileAttributeView;
/*     */ import java.nio.file.attribute.PosixFilePermissions;
/*     */ import java.nio.file.attribute.UserPrincipalLookupService;
/*     */ import java.text.DateFormat;
/*     */ import java.text.SimpleDateFormat;
/*     */ import java.util.ArrayList;
/*     */ import java.util.Arrays;
/*     */ import java.util.Collections;
/*     */ import java.util.Date;
/*     */ import java.util.HashMap;
/*     */ import java.util.HashSet;
/*     */ import java.util.Iterator;
/*     */ import java.util.List;
/*     */ import java.util.Locale;
/*     */ import java.util.Map;
/*     */ import java.util.Set;
/*     */ import java.util.concurrent.ConcurrentHashMap;
/*     */ import java.util.concurrent.TimeUnit;
/*     */ import java.util.regex.Matcher;
/*     */ import java.util.regex.Pattern;
/*     */ import org.apache.nifi.annotation.behavior.EventDriven;
/*     */ import org.apache.nifi.annotation.behavior.InputRequirement;
/*     */ import org.apache.nifi.annotation.behavior.ReadsAttribute;
/*     */ import org.apache.nifi.annotation.behavior.ReadsAttributes;
/*     */ import org.apache.nifi.annotation.behavior.Restricted;
/*     */ import org.apache.nifi.annotation.behavior.Restriction;
/*     */ import org.apache.nifi.annotation.behavior.SupportsBatching;
/*     */ import org.apache.nifi.annotation.behavior.WritesAttribute;
/*     */ import org.apache.nifi.annotation.behavior.WritesAttributes;
/*     */ import org.apache.nifi.annotation.documentation.CapabilityDescription;
/*     */ import org.apache.nifi.annotation.documentation.Tags;
/*     */ import org.apache.nifi.components.PropertyDescriptor;
/*     */ import org.apache.nifi.components.RequiredPermission;
/*     */ import org.apache.nifi.components.ValidationContext;
/*     */ import org.apache.nifi.components.ValidationResult;
/*     */ import org.apache.nifi.components.Validator;
/*     */ import org.apache.nifi.expression.ExpressionLanguageScope;
/*     */ import org.apache.nifi.flowfile.FlowFile;
/*     */ import org.apache.nifi.flowfile.attributes.CoreAttributes;
/*     */ import org.apache.nifi.flowfile.attributes.FragmentAttributes;
/*     */ import org.apache.nifi.logging.ComponentLog;
/*     */ import org.apache.nifi.processor.AbstractProcessor;
/*     */ import org.apache.nifi.processor.ProcessContext;
/*     */ import org.apache.nifi.processor.ProcessSession;
/*     */ import org.apache.nifi.processor.ProcessorInitializationContext;
/*     */ import org.apache.nifi.processor.Relationship;
/*     */ import org.apache.nifi.processor.exception.ProcessException;
/*     */ import org.apache.nifi.processor.util.StandardValidators;
/*     */ import org.apache.nifi.util.StopWatch;
/*     */ 
/*     */ @EventDriven
/*     */ @SupportsBatching
/*     */ @InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
/*     */ @Tags({"put", "local", "copy", "archive", "files", "filesystem"})
/*     */ @CapabilityDescription("Writes the contents of a FlowFile to the local file system")
/*     */ @ReadsAttribute(attribute = "filename", description = "The filename to use when writing the FlowFile to disk.")
/*     */ @Restricted(restrictions = {@Restriction(requiredPermission = RequiredPermission.WRITE_FILESYSTEM, explanation = "Provides operator the ability to write to any file that NiFi has access to.")})
/*     */ @ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
/*     */ @WritesAttributes({@WritesAttribute(attribute = "", description = "")})
/*     */ public class WriteProcessor extends AbstractProcessor {
/*  66 */   public static final String FRAGMENT_ID_ATTRIBUTE = FragmentAttributes.FRAGMENT_ID.key();
/*  67 */   public static final String FRAGMENT_INDEX_ATTRIBUTE = FragmentAttributes.FRAGMENT_INDEX.key();
/*  68 */   public static final String FRAGMENT_COUNT_ATTRIBUTE = FragmentAttributes.FRAGMENT_COUNT.key();
/*     */   
/*     */   public static final String REPLACE_RESOLUTION = "replace";
/*     */   
/*     */   public static final String IGNORE_RESOLUTION = "ignore";
/*     */   
/*     */   public static final String FAIL_RESOLUTION = "fail";
/*     */   public static final String FILE_MODIFY_DATE_ATTRIBUTE = "file.lastModifiedTime";
/*     */   public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
/*  77 */   public static final Pattern RWX_PATTERN = Pattern.compile("^([r-][w-])([x-])([r-][w-])([x-])([r-][w-])([x-])$");
/*  78 */   public static final Pattern NUM_PATTERN = Pattern.compile("^[0-7]{3}$");
/*     */   
/*  80 */   public static final Map<String, Map<String, Object>> WRITEFILE = new HashMap<>();
/*     */   
/*  82 */   private static final Validator PERMISSIONS_VALIDATOR = new Validator()
/*     */     {
/*     */       public ValidationResult validate(String subject, String input, ValidationContext context) {
/*  85 */         ValidationResult.Builder vr = new ValidationResult.Builder();
/*  86 */         if (context.isExpressionLanguagePresent(input)) {
/*  87 */           return (new ValidationResult.Builder()).subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
/*     */         }
/*     */         
/*  90 */         if (WriteProcessor.RWX_PATTERN.matcher(input).matches() || WriteProcessor.NUM_PATTERN.matcher(input).matches()) {
/*  91 */           return vr.valid(true).build();
/*     */         }
/*  93 */         return vr.valid(false)
/*  94 */           .subject(subject)
/*  95 */           .input(input)
/*  96 */           .explanation("This must be expressed in rwxr-x--- form or octal triplet form.")
/*  97 */           .build();
/*     */       }
/*     */     };
/*     */   
/* 101 */   public static final PropertyDescriptor DIRECTORY = (new PropertyDescriptor.Builder())
/* 102 */     .name("Directory")
/* 103 */     .description("The directory to which files should be written. You may use expression language such as /aa/bb/${path}")
/* 104 */     .required(true)
/* 105 */     .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
/* 106 */     .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
/* 107 */     .build();
/* 108 */   public static final PropertyDescriptor MAX_DESTINATION_FILES = (new PropertyDescriptor.Builder())
/* 109 */     .name("Maximum File Count")
/* 110 */     .description("Specifies the maximum number of files that can exist in the output directory")
/* 111 */     .required(false)
/* 112 */     .addValidator(StandardValidators.INTEGER_VALIDATOR)
/* 113 */     .build();
/* 114 */   public static final PropertyDescriptor CONFLICT_RESOLUTION = (new PropertyDescriptor.Builder())
/* 115 */     .name("Conflict Resolution Strategy")
/* 116 */     .description("Indicates what should happen when a file with the same name already exists in the output directory")
/* 117 */     .required(true)
/* 118 */     .defaultValue("fail")
/* 119 */     .allowableValues(new String[] { "replace", "ignore", "fail"
/* 120 */       }).build();
/* 121 */   public static final PropertyDescriptor CHANGE_LAST_MODIFIED_TIME = (new PropertyDescriptor.Builder())
/* 122 */     .name("Last Modified Time")
/* 123 */     .description("Sets the lastModifiedTime on the output file to the value of this attribute.  Format must be yyyy-MM-dd'T'HH:mm:ssZ.  You may also use expression language such as ${file.lastModifiedTime}.")
/*     */     
/* 125 */     .required(false)
/* 126 */     .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
/* 127 */     .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
/* 128 */     .build();
/* 129 */   public static final PropertyDescriptor TIME_OUT = (new PropertyDescriptor.Builder())
/* 130 */     .name("Last Modified Time TimeOut(millisecond) ")
/* 131 */     .description("Task last modified time timeout.")
/* 132 */     .required(true)
/* 133 */     .defaultValue("600000")
/* 134 */     .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
/* 135 */     .build();
/*     */   
/* 137 */   public static final PropertyDescriptor CHANGE_PERMISSIONS = (new PropertyDescriptor.Builder())
/* 138 */     .name("Permissions")
/* 139 */     .description("Sets the permissions on the output file to the value of this attribute.  Format must be either UNIX rwxrwxrwx with a - in place of denied permissions (e.g. rw-r--r--) or an octal number (e.g. 644).  You may also use expression language such as ${file.permissions}.")
/*     */ 
/*     */     
/* 142 */     .required(false)
/* 143 */     .addValidator(PERMISSIONS_VALIDATOR)
/* 144 */     .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
/* 145 */     .build();
/* 146 */   public static final PropertyDescriptor CHANGE_OWNER = (new PropertyDescriptor.Builder())
/* 147 */     .name("Owner")
/* 148 */     .description("Sets the owner on the output file to the value of this attribute.  You may also use expression language such as ${file.owner}. Note on many operating systems Nifi must be running as a super-user to have the permissions to set the file owner.")
/*     */     
/* 150 */     .required(false)
/* 151 */     .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
/* 152 */     .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
/* 153 */     .build();
/* 154 */   public static final PropertyDescriptor CHANGE_GROUP = (new PropertyDescriptor.Builder())
/* 155 */     .name("Group")
/* 156 */     .description("Sets the group on the output file to the value of this attribute.  You may also use expression language such as ${file.group}.")
/*     */     
/* 158 */     .required(false)
/* 159 */     .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
/* 160 */     .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
/* 161 */     .build();
/* 162 */   public static final PropertyDescriptor CREATE_DIRS = (new PropertyDescriptor.Builder())
/* 163 */     .name("Create Missing Directories")
/* 164 */     .description("If true, then missing destination directories will be created. If false, flowfiles are penalized and sent to failure.")
/* 165 */     .required(true)
/* 166 */     .allowableValues(new String[] { "true", "false"
/* 167 */       }).defaultValue("true")
/* 168 */     .build();
/*     */   
/*     */   public static final int MAX_FILE_LOCK_ATTEMPTS = 10;
/* 171 */   public static final Relationship REL_SUCCESS = (new Relationship.Builder())
/* 172 */     .name("success")
/* 173 */     .description("Files that have been successfully written to the output directory are transferred to this relationship")
/* 174 */     .build();
/* 175 */   public static final Relationship REL_FAILURE = (new Relationship.Builder())
/* 176 */     .name("failure")
/* 177 */     .description("Files that could not be written to the output directory for some reason are transferred to this relationship")
/* 178 */     .build();
/* 179 */   public static final Relationship REL_FRAGMENT_SUCCESS = (new Relationship.Builder())
/* 180 */     .name("fragment success")
/* 181 */     .description("Files fragment that have been successfully written to the output directory are transferred to this relationship")
/* 182 */     .build();
/*     */   
/*     */   private List<PropertyDescriptor> properties;
/*     */   
/*     */   private Set<Relationship> relationships;
/*     */ 
/*     */   
/*     */   protected void init(ProcessorInitializationContext context) {
/* 190 */     Set<Relationship> procRels = new HashSet<>();
/* 191 */     procRels.add(REL_SUCCESS);
/* 192 */     procRels.add(REL_FAILURE);
/* 193 */     procRels.add(REL_FRAGMENT_SUCCESS);
/* 194 */     this.relationships = Collections.unmodifiableSet(procRels);
/*     */ 
/*     */     
/* 197 */     List<PropertyDescriptor> supDescriptors = new ArrayList<>();
/* 198 */     supDescriptors.add(DIRECTORY);
/* 199 */     supDescriptors.add(CONFLICT_RESOLUTION);
/* 200 */     supDescriptors.add(CREATE_DIRS);
/* 201 */     supDescriptors.add(MAX_DESTINATION_FILES);
/* 202 */     supDescriptors.add(CHANGE_LAST_MODIFIED_TIME);
/* 203 */     supDescriptors.add(TIME_OUT);
/* 204 */     supDescriptors.add(CHANGE_PERMISSIONS);
/* 205 */     supDescriptors.add(CHANGE_OWNER);
/* 206 */     supDescriptors.add(CHANGE_GROUP);
/* 207 */     this.properties = Collections.unmodifiableList(supDescriptors);
/*     */   }
/*     */ 
/*     */   
/*     */   public Set<Relationship> getRelationships() {
/* 212 */     return this.relationships;
/*     */   }
/*     */ 
/*     */   
/*     */   protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
/* 217 */     return this.properties;
/*     */   }
/*     */ 
/*     */   
/*     */   public void onTrigger(ProcessContext context, ProcessSession session) {
/* 222 */     FlowFile flowFile = session.get();
/* 223 */     if (flowFile == null) {
/*     */       return;
/*     */     }
/*     */     
/* 227 */     StopWatch stopWatch = new StopWatch(true);
/* 228 */     Path configuredRootDirPath = Paths.get(context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue(), new String[0]);
/* 229 */     String conflictResponse = context.getProperty(CONFLICT_RESOLUTION).getValue();
/* 230 */     Integer maxDestinationFiles = context.getProperty(MAX_DESTINATION_FILES).asInteger();
/* 231 */     ComponentLog logger = getLogger();
/* 232 */     int time_out = context.getProperty(TIME_OUT).asInteger().intValue();
/*     */ 
/*     */     
/* 235 */     String check_task_time_out = flowFile.getAttribute("check_task_time_out");
/* 236 */     if (check_task_time_out != null) {
/* 237 */       synchronized (WRITEFILE) {
/* 238 */         Iterator<Map.Entry<String, Map<String, Object>>> iterator = WRITEFILE.entrySet().iterator();
/* 239 */         while (iterator.hasNext()) {
/* 240 */           Map.Entry<String, Map<String, Object>> entry = iterator.next();
/* 241 */           Map v = entry.getValue();
/* 242 */           if (System.currentTimeMillis() - ((Long)v.get("lasttime")).longValue() > time_out) {
/* 243 */             getLogger().debug("iterator:{} ", new Object[] { iterator });
/* 244 */             iterator.remove();
/*     */           } 
/*     */         } 
/*     */       } 
/* 248 */       session.transfer(flowFile, REL_SUCCESS);
/*     */       
/*     */       return;
/*     */     } 
/* 252 */     Path tempDotCopyFile = null; try {
/*     */       Map<String, Object> fileinfo;
/* 254 */       Path rootDirPath = configuredRootDirPath.toAbsolutePath();
/* 255 */       String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
/* 256 */       Path tempCopyFile = rootDirPath.resolve("." + filename);
/* 257 */       Path copyFile = rootDirPath.resolve(filename);
/*     */       
/* 259 */       String permissions = context.getProperty(CHANGE_PERMISSIONS).evaluateAttributeExpressions(flowFile).getValue();
/* 260 */       String owner = context.getProperty(CHANGE_OWNER).evaluateAttributeExpressions(flowFile).getValue();
/* 261 */       String group = context.getProperty(CHANGE_GROUP).evaluateAttributeExpressions(flowFile).getValue();
/*     */       
/* 263 */       String fragment_identifier = flowFile.getAttribute(FRAGMENT_ID_ATTRIBUTE);
/* 264 */       String fragment_index = flowFile.getAttribute(FRAGMENT_INDEX_ATTRIBUTE);
/* 265 */       String fragment_count = flowFile.getAttribute(FRAGMENT_COUNT_ATTRIBUTE);
/* 266 */       Long fragment_countl = Long.valueOf(0L);
/* 267 */       if (fragment_count != null) {
/* 268 */         fragment_countl = Long.valueOf(fragment_count);
/*     */       }
/*     */ 
/*     */       
/* 272 */       synchronized (WRITEFILE) {
/* 273 */         if (WRITEFILE.containsKey(fragment_identifier)) {
/* 274 */           fileinfo = WRITEFILE.get(fragment_identifier);
/*     */         } else {
/* 276 */           fileinfo = new ConcurrentHashMap<>();
/* 277 */           fileinfo.put("count", Long.valueOf(0L));
/* 278 */           WRITEFILE.put(fragment_identifier, fileinfo);
/*     */         } 
/*     */       } 
/*     */       
/* 282 */       if (!Files.exists(rootDirPath, new java.nio.file.LinkOption[0])) {
/* 283 */         if (context.getProperty(CREATE_DIRS).asBoolean().booleanValue()) {
/* 284 */           Path existing = rootDirPath;
/* 285 */           while (!Files.exists(existing, new java.nio.file.LinkOption[0])) {
/* 286 */             existing = existing.getParent();
/*     */           }
/* 288 */           if (permissions != null && !permissions.trim().isEmpty()) {
/*     */             try {
/* 290 */               String perms = stringPermissions(permissions, true);
/* 291 */               if (!perms.isEmpty()) {
/* 292 */                 Files.createDirectories(rootDirPath, (FileAttribute<?>[])new FileAttribute[] { PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(perms)) });
/*     */               } else {
/* 294 */                 Files.createDirectories(rootDirPath, (FileAttribute<?>[])new FileAttribute[0]);
/*     */               } 
/* 296 */             } catch (Exception e) {
/* 297 */               flowFile = session.penalize(flowFile);
/* 298 */               session.transfer(flowFile, REL_FAILURE);
/* 299 */               logger.error("Could not set create directory with permissions {} because {}", new Object[] { permissions, e });
/*     */               return;
/*     */             } 
/*     */           } else {
/* 303 */             Files.createDirectories(rootDirPath, (FileAttribute<?>[])new FileAttribute[0]);
/*     */           } 
/*     */           
/* 306 */           boolean chOwner = (owner != null && !owner.trim().isEmpty());
/* 307 */           boolean chGroup = (group != null && !group.trim().isEmpty());
/* 308 */           if (chOwner || chGroup) {
/* 309 */             Path currentPath = rootDirPath;
/* 310 */             while (!currentPath.equals(existing)) {
/* 311 */               if (chOwner) {
/*     */                 try {
/* 313 */                   UserPrincipalLookupService lookupService = currentPath.getFileSystem().getUserPrincipalLookupService();
/* 314 */                   Files.setOwner(currentPath, lookupService.lookupPrincipalByName(owner));
/* 315 */                 } catch (Exception e) {
/* 316 */                   logger.warn("Could not set directory owner to {} because {}", new Object[] { owner, e });
/*     */                 } 
/*     */               }
/* 319 */               if (chGroup) {
/*     */                 try {
/* 321 */                   UserPrincipalLookupService lookupService = currentPath.getFileSystem().getUserPrincipalLookupService();
/* 322 */                   PosixFileAttributeView view = Files.<PosixFileAttributeView>getFileAttributeView(currentPath, PosixFileAttributeView.class, new java.nio.file.LinkOption[0]);
/* 323 */                   view.setGroup(lookupService.lookupPrincipalByGroupName(group));
/* 324 */                 } catch (Exception e) {
/* 325 */                   logger.warn("Could not set file group to {} because {}", new Object[] { group, e });
/*     */                 } 
/*     */               }
/* 328 */               currentPath = currentPath.getParent();
/*     */             } 
/*     */           } 
/*     */         } else {
/* 332 */           flowFile = session.penalize(flowFile);
/* 333 */           session.transfer(flowFile, REL_FAILURE);
/* 334 */           logger.error("Penalizing {} and routing to 'failure' because the output directory {} does not exist and Processor is configured not to create missing directories", new Object[] { flowFile, rootDirPath });
/*     */           
/*     */           return;
/*     */         } 
/*     */       }
/*     */       
/* 340 */       Path dotCopyFile = tempCopyFile;
/* 341 */       tempDotCopyFile = dotCopyFile;
/* 342 */       Path finalCopyFile = copyFile;
/*     */       
/* 344 */       Path finalCopyFileDir = finalCopyFile.getParent();
/* 345 */       if (Files.exists(finalCopyFileDir, new java.nio.file.LinkOption[0]) && maxDestinationFiles != null) {
/* 346 */         long numFiles = getFilesNumberInFolder(finalCopyFileDir, filename);
/*     */         
/* 348 */         if (numFiles >= maxDestinationFiles.intValue()) {
/* 349 */           flowFile = session.penalize(flowFile);
/* 350 */           logger.warn("Penalizing {} and routing to 'failure' because the output directory {} has {} files, which exceeds the configured maximum number of files", new Object[] { flowFile, finalCopyFileDir, 
/* 351 */                 Long.valueOf(numFiles) });
/* 352 */           session.transfer(flowFile, REL_FAILURE);
/*     */           
/*     */           return;
/*     */         } 
/*     */       } 
/* 357 */       if (Files.exists(finalCopyFile, new java.nio.file.LinkOption[0])) {
/* 358 */         switch (conflictResponse) {
/*     */           case "replace":
/* 360 */             Files.delete(finalCopyFile);
/* 361 */             logger.info("Deleted {} as configured in order to replace with the contents of {}", new Object[] { finalCopyFile, flowFile });
/*     */             break;
/*     */           case "ignore":
/* 364 */             session.transfer(flowFile, REL_SUCCESS);
/* 365 */             logger.info("Transferring {} to success because file with same name already exists", new Object[] { flowFile });
/*     */             return;
/*     */           case "fail":
/* 368 */             flowFile = session.penalize(flowFile);
/* 369 */             logger.warn("Penalizing {} and routing to failure as configured because file with the same name already exists", new Object[] { flowFile });
/* 370 */             session.transfer(flowFile, REL_FAILURE);
/*     */             return;
/*     */         } 
/*     */ 
/*     */ 
/*     */ 
/*     */       
/*     */       }
/* 378 */       if (fragment_index != null) {
/* 379 */         session.exportTo(flowFile, dotCopyFile, true);
/*     */       }
/*     */       
/* 382 */       boolean End_o_processing = false;
/* 383 */       synchronized (WRITEFILE) {
/* 384 */         Long count = Long.valueOf(((Long)fileinfo.get("count")).longValue() + ((fragment_index != null) ? 1L : 0L));
/* 385 */         fileinfo.put("count", count);
/* 386 */         fileinfo.put("lasttime", Long.valueOf(System.currentTimeMillis()));
/* 387 */         if (fileinfo.containsKey(FRAGMENT_COUNT_ATTRIBUTE) && fragment_countl.longValue() == 0L) {
/* 388 */           fragment_countl = (Long)fileinfo.get(FRAGMENT_COUNT_ATTRIBUTE);
/*     */         }
/* 390 */         getLogger().debug("tempCopyFile:{} \ncopyFile:{} \nfilename:{} \nfinalCopyFile:{}\n fragment_countl:{} count:{} \n fragment_countl==count:{}  \n line:{}", new Object[] { tempCopyFile, copyFile, filename, finalCopyFile, fragment_countl, count, flowFile });
/* 391 */         if (fragment_countl.longValue() == count.longValue()) {
/* 392 */           End_o_processing = true;
/* 393 */           WRITEFILE.remove(fragment_identifier);
/*     */         } else {
/* 395 */           fileinfo.put(FRAGMENT_COUNT_ATTRIBUTE, fragment_countl);
/* 396 */           session.transfer(flowFile, REL_FRAGMENT_SUCCESS);
/*     */           
/*     */           return;
/*     */         } 
/*     */       } 
/* 401 */       if (!End_o_processing) {
/*     */         return;
/*     */       }
/*     */       
/* 405 */       String lastModifiedTime = context.getProperty(CHANGE_LAST_MODIFIED_TIME).evaluateAttributeExpressions(flowFile).getValue();
/* 406 */       if (lastModifiedTime != null && !lastModifiedTime.trim().isEmpty()) {
/*     */         try {
/* 408 */           DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.US);
/* 409 */           Date fileModifyTime = formatter.parse(lastModifiedTime);
/* 410 */           dotCopyFile.toFile().setLastModified(fileModifyTime.getTime());
/* 411 */         } catch (Exception e) {
/* 412 */           logger.warn("Could not set file lastModifiedTime to {} because {}", new Object[] { lastModifiedTime, e });
/*     */         } 
/*     */       }
/*     */       
/* 416 */       if (permissions != null && !permissions.trim().isEmpty()) {
/*     */         try {
/* 418 */           String perms = stringPermissions(permissions, false);
/* 419 */           if (!perms.isEmpty()) {
/* 420 */             Files.setPosixFilePermissions(dotCopyFile, PosixFilePermissions.fromString(perms));
/*     */           }
/* 422 */         } catch (Exception e) {
/* 423 */           logger.warn("Could not set file permissions to {} because {}", new Object[] { permissions, e });
/*     */         } 
/*     */       }
/*     */       
/* 427 */       if (owner != null && !owner.trim().isEmpty()) {
/*     */         try {
/* 429 */           UserPrincipalLookupService lookupService = dotCopyFile.getFileSystem().getUserPrincipalLookupService();
/* 430 */           Files.setOwner(dotCopyFile, lookupService.lookupPrincipalByName(owner));
/* 431 */         } catch (Exception e) {
/* 432 */           logger.warn("Could not set file owner to {} because {}", new Object[] { owner, e });
/*     */         } 
/*     */       }
/*     */       
/* 436 */       if (group != null && !group.trim().isEmpty()) {
/*     */         try {
/* 438 */           UserPrincipalLookupService lookupService = dotCopyFile.getFileSystem().getUserPrincipalLookupService();
/* 439 */           PosixFileAttributeView view = Files.<PosixFileAttributeView>getFileAttributeView(dotCopyFile, PosixFileAttributeView.class, new java.nio.file.LinkOption[0]);
/* 440 */           view.setGroup(lookupService.lookupPrincipalByGroupName(group));
/* 441 */         } catch (Exception e) {
/* 442 */           logger.warn("Could not set file group to {} because {}", new Object[] { group, e });
/*     */         } 
/*     */       }
/*     */       
/* 446 */       boolean renamed = false;
/* 447 */       for (int i = 0; i < 10; i++) {
/* 448 */         if (dotCopyFile.toFile().renameTo(finalCopyFile.toFile())) {
/* 449 */           renamed = true;
/*     */           break;
/*     */         } 
/* 452 */         Thread.sleep(100L);
/*     */       } 
/*     */       
/* 455 */       if (!renamed) {
/* 456 */         if (Files.exists(dotCopyFile, new java.nio.file.LinkOption[0]) && dotCopyFile.toFile().delete()) {
/* 457 */           logger.debug("Deleted dot copy file {}", new Object[] { dotCopyFile });
/*     */         }
/* 459 */         throw new ProcessException("Could not rename: " + dotCopyFile);
/*     */       } 
/* 461 */       logger.info("Produced copy of {} at location {}", new Object[] { flowFile, finalCopyFile });
/*     */ 
/*     */       
/* 464 */       session.getProvenanceReporter().send(flowFile, finalCopyFile.toFile().toURI().toString(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
/* 465 */       session.transfer(flowFile, REL_SUCCESS);
/* 466 */     } catch (Throwable t) {
/* 467 */       if (tempDotCopyFile != null) {
/*     */         try {
/* 469 */           Files.deleteIfExists(tempDotCopyFile);
/* 470 */         } catch (Exception e) {
/* 471 */           logger.error("Unable to remove temporary file {} due to {}", new Object[] { tempDotCopyFile, e });
/*     */         } 
/*     */       }
/*     */       
/* 475 */       flowFile = session.penalize(flowFile);
/* 476 */       logger.error("Penalizing {} and transferring to failure due to {}", new Object[] { flowFile, t });
/* 477 */       session.transfer(flowFile, REL_FAILURE);
/*     */     } 
/*     */   }
/*     */   
/*     */   private long getFilesNumberInFolder(Path folder, String filename) {
/* 482 */     String[] filesInFolder = folder.toFile().list();
/* 483 */     return Arrays.<String>stream(filesInFolder)
/* 484 */       .filter(eachFilename -> !eachFilename.equals(filename))
/* 485 */       .count();
/*     */   }
/*     */   
/*     */   protected String stringPermissions(String perms, boolean directory) {
/* 489 */     String permissions = "";
/* 490 */     Matcher rwx = RWX_PATTERN.matcher(perms);
/* 491 */     if (rwx.matches()) {
/* 492 */       if (directory) {
/*     */         
/* 494 */         StringBuilder permBuilder = new StringBuilder();
/* 495 */         permBuilder.append("$1");
/* 496 */         permBuilder.append(rwx.group(1).equals("--") ? "$2" : "x");
/* 497 */         permBuilder.append("$3");
/* 498 */         permBuilder.append(rwx.group(3).equals("--") ? "$4" : "x");
/* 499 */         permBuilder.append("$5");
/* 500 */         permBuilder.append(rwx.group(5).equals("--") ? "$6" : "x");
/* 501 */         permissions = rwx.replaceAll(permBuilder.toString());
/*     */       } else {
/* 503 */         permissions = perms;
/*     */       } 
/* 505 */     } else if (NUM_PATTERN.matcher(perms).matches()) {
/*     */       try {
/* 507 */         int number = Integer.parseInt(perms, 8);
/* 508 */         StringBuilder permBuilder = new StringBuilder();
/* 509 */         if ((number & 0x100) > 0) {
/* 510 */           permBuilder.append('r');
/*     */         } else {
/* 512 */           permBuilder.append('-');
/*     */         } 
/* 514 */         if ((number & 0x80) > 0) {
/* 515 */           permBuilder.append('w');
/*     */         } else {
/* 517 */           permBuilder.append('-');
/*     */         } 
/* 519 */         if (directory || (number & 0x40) > 0) {
/* 520 */           permBuilder.append('x');
/*     */         } else {
/* 522 */           permBuilder.append('-');
/*     */         } 
/* 524 */         if ((number & 0x20) > 0) {
/* 525 */           permBuilder.append('r');
/*     */         } else {
/* 527 */           permBuilder.append('-');
/*     */         } 
/* 529 */         if ((number & 0x10) > 0) {
/* 530 */           permBuilder.append('w');
/*     */         } else {
/* 532 */           permBuilder.append('-');
/*     */         } 
/* 534 */         if ((number & 0x8) > 0) {
/* 535 */           permBuilder.append('x');
/*     */         }
/* 537 */         else if (directory && (number & 0x30) > 0) {
/*     */           
/* 539 */           permBuilder.append('x');
/*     */         } else {
/* 541 */           permBuilder.append('-');
/*     */         } 
/*     */         
/* 544 */         if ((number & 0x4) > 0) {
/* 545 */           permBuilder.append('r');
/*     */         } else {
/* 547 */           permBuilder.append('-');
/*     */         } 
/* 549 */         if ((number & 0x2) > 0) {
/* 550 */           permBuilder.append('w');
/*     */         } else {
/* 552 */           permBuilder.append('-');
/*     */         } 
/* 554 */         if ((number & 0x1) > 0) {
/* 555 */           permBuilder.append('x');
/*     */         }
/* 557 */         else if (directory && (number & 0x6) > 0) {
/*     */           
/* 559 */           permBuilder.append('x');
/*     */         } else {
/* 561 */           permBuilder.append('-');
/*     */         } 
/*     */         
/* 564 */         permissions = permBuilder.toString();
/* 565 */       } catch (NumberFormatException numberFormatException) {}
/*     */     } 
/*     */ 
/*     */     
/* 569 */     return permissions;
/*     */   }
/*     */ }
