from abc import ABC, abstractmethod
import redis
import os
import re



class MAFS_Manager(ABC):
    """Abstract class for the main MAFS manager."""

    def __init__(self,
                 ram_fs_manager,
                 disk_fs_manager):

        self.ram_fs_manager = ram_fs_manager
        self.disk_fs_manager = disk_fs_manager
        self.cwd = './root'

    def fs_init(self):
        
        print('initializing MAFS..')
        
        root_tokens = self.parse_directory_path('./root')
        vram_tokens = self.parse_directory_path('./root/vram')
        vdisk_tokens = self.parse_directory_path('./root/vdisk')
        
        vram_log_tokens = self.parse_directory_path('./root/vram/log.txt')

        self.ram_fs_manager.mkdir(vram_tokens)
        self.ram_fs_manager.mkdir(vdisk_tokens)
        
        self.ram_fs_manager.touch(vram_log_tokens)
        self.cwd = './root/vram' 
        f = open('./info.txt','r').read()
        print(f)
        self.ram_fs_manager.ls(vram_tokens)
        


    def display_shell(self, 
                      shell_type = "default"):
        if shell_type == "default":
            cwd = self.cwd.strip('.')
            prompt = f"mafs_shell>:{cwd} "
        else:
            # Handle other shell types if necessary
            prompt = f"{directory}"

        while True:
            try:
                # Display the prompt and take user input
                user_input = input(prompt).strip()
                # Parse and handle the user command
                tokens = self.parse_command(user_input)
                #all subdirectories under /vroot will be in RAM    
                if self.cwd.split('/')[2] == 'vram':
                    if tokens[0] == 'cd': 
                        self.cwd = self.ram_fs_manager.cd(tokens)
                        cwd = self.cwd.strip('.')
                        prompt = f"mafs_shell>:/{cwd} "
                        #import pdb ; pdb.set_trace()
                    
                    if tokens[0] == 'ls':
                        import pdb ; pdb.set_trace()
                        tmp_cwd = self.ram_fs_manager.cd(tokens)
                        


                    exec('self.ram_fs_manager.{}(tokens)'.format(tokens[0])) #parent cmd corresponds to method name
                    
                else:
                    exec('self.disk_fs_manager.{}(tokens)'.format(tokens[0])) #parent cmd corresponds to method name 


            except KeyboardInterrupt:
                # Handle Ctrl+C interrupt gracefully
                print("\nExiting MAFS shell.")
                break
            except Exception as e:
                # Handle other exceptions
                print(f"Error: {e}")
                continue

    #@abstractmethod
    def allocate_task(self):
        """Method to allocate tasks to RAMFSManager or DiskManager."""
        pass
    
    def parse_directory_path(self, path):
        # Split the path into tokens
        tokens = path.split('/')

        # Process tokens to determine directory structure
        processed_tokens = []
        for token in tokens:
            if token == '..':
                # Indicates moving up a directory
                processed_tokens.append('..')
            elif token == '.':
                # Indicates current directory, can be skipped or used for clarity
                processed_tokens.append('.')
            elif token:
                # Regular directory name
                processed_tokens.append(token)

        return processed_tokens

    def parse_command(self, command):
        # Regular expression pattern to match the parent tokens
        pattern = r"\s*(ls|mkdir|touch|rm|cd|mv|cp|exit)\s*"
        matches = re.split(pattern, command.strip())[1:]
        if matches[0] == 'exit':
            exit()

        if len(matches) >= 2:
            # Extract the token and the rest of the command
            parent_cmd, rest = matches[0], matches[1].strip()
            if rest[0] in ['/', '.']: 
                dir_tokens = self.parse_directory_path(rest)
                tokens = [parent_cmd] + dir_tokens
                return tokens
             
        else:
            # No recognized parent token found
            tokens = []
        return tokens
   
    
class RAMFS_Manager(ABC):
    """Abstract class for managing the in-memory file system."""

    def __init__(self, init_config):
        pass
        
    @abstractmethod
    def mkdir(self,tokens):
        pass
    
    @abstractmethod
    def touch(self,tokens): 
        pass

    @abstractmethod
    def ls(self,tokens):
        pass

    @abstractmethod
    def cd(self,tokens):
        pass

    @abstractmethod
    def rm(self,tokens):
        pass

    @abstractmethod
    def mv(self,tokens):
        pass

    @abstractmethod
    def cp(self,tokens):
        pass


class DiskManager(ABC):
    """Abstract class for managing disk storage file system."""

    def __init__(self, init_config):
        self.init_config = init_config

    @abstractmethod
    def manage_disk(self):
        """Method to manage disk storage."""
        pass

class RedisFS(RAMFS_Manager):
    def __init__(self,
                 redis_host,
                 redis_port,
                 cwd = '/vram'):

        self.redis_client = redis.Redis(host=redis_host, port=redis_port)
        self.ram_cwd = cwd
    
    def cd(self,directory_tokens):
        
        if directory_tokens[1] == '..':
            parent = self.redis_client.hget(self.ram_cwd.strip('/'),'parent').decode('utf8')
            return parent 
        else:
            pass
            

    def mv(self):
        pass

    def rm(self):
        pass

    def cp(self):
        pass

    def ls(self, directory_tokens):
        
        # Get all fields in the HASH
        all_fields = self.redis_client.hgetall(directory_tokens[-1])
        # Filter out child directories and files
        directories = [value.decode('utf-8') for key, value in all_fields.items() if key.decode('utf-8').startswith('child')]
        files = [key.decode('utf-8') for key, value in all_fields.items() if not key.decode('utf-8').startswith('child')]

        # Display in UNIX style
        for d in directories:
            print(f"{d}/")  # Directories end with a slash
        for file in files:
            print(file)  # Files are displayed as is
    
    def mkfile(self,directory_tokens,contents):

        file_name = self.redis_client.hgetall(directory_tokens[-1])
        directory = directory_tokens[1:-2]
        self.redis_client.hset(directory,file_name,contents)

    def touch(self,directory_tokens): 
        
        dir_ = directory_tokens[-2]
        file_name = directory_tokens[-1]
        self.redis_client.hset(dir_,file_name,'NULL')


    def mkdir(self, directory_tokens):
        parent = 'NULL' if directory_tokens[1] == 'vram' else directory_tokens[0]
        for i, dir_name in enumerate(directory_tokens[1:], 1):
            # Count the number of existing 'child*' fields
            cursor, child_fields = self.redis_client.hscan(dir_name, match='child*')
            child_count = len(child_fields)

            # Determine the next child field
            child_field_name = f'child{child_count + 1}'
            child = directory_tokens[i+1] if i+1 < len(directory_tokens) else 'NULL'

            # Set the parent and next child for each directory
            self.redis_client.hset(dir_name, 'parent', parent)
            self.redis_client.hset(dir_name, child_field_name, child)

            parent = dir_name

ram_fs_manager = RedisFS(redis_host = 'localhost',
                                redis_port = 6379)
mgr = MAFS_Manager(ram_fs_manager,
           disk_fs_manager = 'lol, cheating')
mgr.fs_init()
mgr.display_shell(shell_type = 'default')

