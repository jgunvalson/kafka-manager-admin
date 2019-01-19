package admin.ops.converter

import kafkamanager.model.DeleteAcl
import org.apache.kafka.common.acl.AccessControlEntryFilter
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePatternFilter
import org.apache.kafka.common.resource.ResourceType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component


@Component
class DeleteResourceAclConverter {

    companion object {
        private val log: Logger = LoggerFactory.getLogger(DeleteResourceAclConverter::class.java)
    }


    fun convertDeleteResourceAcl(deleteAcl: DeleteAcl): AclBindingFilter {


        val resourceType = when (deleteAcl.getResourceType().toLowerCase()) {
            "topic" ->  ResourceType.TOPIC
            "group" ->  ResourceType.GROUP
            "cluster" ->  ResourceType.CLUSTER
            else -> {
                ResourceType.UNKNOWN
            }
        }


        val patternType = when (deleteAcl.getPatternType().toLowerCase()) {
            "literal" -> PatternType.LITERAL
            "prefixed" -> PatternType.PREFIXED
            "match" -> PatternType.MATCH
            else -> {
                PatternType.UNKNOWN
            }
        }


        val aclOperation = when (deleteAcl.getOperation().toLowerCase()) {
            "read" -> AclOperation.READ
            "write" -> AclOperation.WRITE
            "describe" -> AclOperation.DESCRIBE
            "describe_configs" -> AclOperation.DESCRIBE_CONFIGS
            "alter" -> AclOperation.ALTER
            "cluster_action" -> AclOperation.CLUSTER_ACTION
            "idempotent_write" -> AclOperation.IDEMPOTENT_WRITE
            "all" -> AclOperation.ALL
            else -> {
                AclOperation.UNKNOWN
            }
        }

        val aclPermissionType = when (deleteAcl.getPermissionType().toLowerCase()) {
            "allow" -> AclPermissionType.ALLOW
            "deny" -> AclPermissionType.DENY
            "any" -> AclPermissionType.ANY
            else -> {
                AclPermissionType.UNKNOWN
            }
        }


        return AclBindingFilter(
                ResourcePatternFilter(resourceType, deleteAcl.getResource(), patternType),
                AccessControlEntryFilter(deleteAcl.getPrincipal(), deleteAcl.getHost(), aclOperation, aclPermissionType)
        )
    }
}