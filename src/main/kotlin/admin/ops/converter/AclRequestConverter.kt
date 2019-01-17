package admin.ops.converter

import kafkamanager.model.AclRequest
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class AclRequestConverter {

    companion object {
        private val log: Logger = LoggerFactory.getLogger(AclRequestConverter::class.java)
    }

    fun convertAclRequest(aclRequest: AclRequest): AclBinding {


        val resourceType = when (aclRequest.getResourceType().toLowerCase()) {
            "topic" ->  ResourceType.TOPIC
            "group" ->  ResourceType.GROUP
            "cluster" ->  ResourceType.CLUSTER
            else -> {
                 ResourceType.UNKNOWN
            }
        }


        val patternType = when (aclRequest.getPatternType().toLowerCase()) {
            "literal" -> PatternType.LITERAL
            "prefixed" -> PatternType.PREFIXED
            "match" -> PatternType.MATCH
            else -> {
                PatternType.UNKNOWN
            }
        }


        val aclOperation = when (aclRequest.getOperation().toLowerCase()) {
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

        val aclPermissionType = when (aclRequest.getPermissionType().toLowerCase()) {
            "allow" -> AclPermissionType.ALLOW
            "deny" -> AclPermissionType.DENY
            "any" -> AclPermissionType.ANY
            else -> {
                AclPermissionType.UNKNOWN
            }
        }


        val resourcePattern = ResourcePattern(resourceType,
                aclRequest.getPrincipal(),
                patternType)

        val accessControlEntry = AccessControlEntry(aclRequest.getPrincipal(),
                aclRequest.getHost(),
                aclOperation,
                aclPermissionType)

        return AclBinding(resourcePattern, accessControlEntry)
    }

}