import subprocess
import sys
import time
import logging
import signal
import os
import threading
import psutil
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor
import re

class SimpleBotRunner:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize simple bot runner without AI features"""
        default_config = {
            'bot_files': ['enze.py'],
            'max_restarts': 10,
            'restart_delay': 15,
            'max_restart_delay': 300,
            'log_file': 'bot_runner.log',
            'monitor_resources': True,
            'smart_log_filtering': True,
            'memory_threshold_mb': 500,
            'restart_on_high_memory': True,
            'parallel_execution': True,
        }
        
        self.config = {**default_config, **(config or {})}
        self.current_processes = {}
        self.shutdown_requested = False
        self.file_loggers = {}
        self.restart_counts = {}
        self.executor = ThreadPoolExecutor(max_workers=10)
        
        self.setup_logging()
        
        # Signal handling
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.logger.info("🤖 Simple Bot Runner initialized")
        self.logger.info(f"Managing {len(self.config['bot_files'])} bot files")

    def setup_logging(self):
        """Setup logging system"""
        try:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(self.config['log_file'], encoding='utf-8'),
                    logging.StreamHandler()
                ]
            )
            self.logger = logging.getLogger("SimpleBotRunner")
        except Exception as e:
            print(f"Failed to setup logging: {e}")
            self.logger = logging.getLogger("SimpleBotRunner")

    def create_file_logger(self, file_path: Path) -> logging.Logger:
        """Create logger for specific file"""
        file_name = file_path.stem
        logger_name = f"Bot_{file_name}"
        
        if logger_name in self.file_loggers:
            return self.file_loggers[logger_name]
        
        try:
            file_logger = logging.getLogger(logger_name)
            file_logger.setLevel(logging.INFO)
            
            formatter = logging.Formatter(f'%(asctime)s - [{file_name}] %(message)s')
            
            # File handler
            file_handler = logging.FileHandler(f'{file_name}.log', encoding='utf-8')
            file_handler.setFormatter(formatter)
            
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            
            file_logger.addHandler(file_handler)
            file_logger.addHandler(console_handler)
            file_logger.propagate = False
            
            self.file_loggers[logger_name] = file_logger
            return file_logger
        except Exception as e:
            self.logger.error(f"Failed to create logger for {file_name}: {e}")
            return self.logger

    def is_real_error(self, message: str) -> bool:
        """Simple error detection"""
        if not self.config['smart_log_filtering']:
            return True
        
        # Simple patterns for non-errors
        non_error_patterns = [
            r'INFO|info|Debug|debug|DEBUG',
            r'Successfully|successfully|SUCCESS',
            r'Started|started|Starting|starting',
            r'Initialized|initialized|Connected|connected',
            r'Polling|polling|Running|running'
        ]
        
        # Check if it's NOT an error
        for pattern in non_error_patterns:
            if re.search(pattern, message, re.IGNORECASE):
                return False
        
        # Simple error patterns
        error_patterns = [
            r'ERROR|Error|error|CRITICAL|Critical|critical',
            r'Exception|exception|Traceback|traceback',
            r'Failed|failed|FAILED'
        ]
        
        for pattern in error_patterns:
            if re.search(pattern, message, re.IGNORECASE):
                return True
        
        return False

    def should_restart(self, exit_code: int) -> bool:
        """Simple restart logic based on exit code"""
        # Don't restart on normal exit
        if exit_code == 0:
            return False
        
        # Don't restart on manual termination
        if exit_code in [130, 143, -15]:  # SIGINT, SIGTERM
            return False
        
        # Restart on other exit codes
        return True

    def calculate_restart_delay(self, file_path: Path) -> int:
        """Calculate restart delay"""
        restart_count = self.restart_counts.get(str(file_path), 0)
        
        # Progressive delay
        delay = min(
            self.config['restart_delay'] * (restart_count + 1),
            self.config['max_restart_delay']
        )
        
        return delay

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"🛑 Received signal {signum}. Shutting down...")
        self.shutdown_requested = True
        self.stop_all_processes()

    def start_file_process(self, file_path: Path) -> Optional[subprocess.Popen]:
        """Start file as process"""
        if not file_path.exists():
            self.logger.error(f"❌ File {file_path} not found!")
            return None
        
        file_logger = self.create_file_logger(file_path)
        
        try:
            file_logger.info(f"🚀 Starting process...")
            
            process = subprocess.Popen(
                [sys.executable, str(file_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True,
                cwd=file_path.parent
            )
            
            file_logger.info(f"✅ Process started with PID: {process.pid}")
            return process
            
        except Exception as e:
            file_logger.error(f"❌ Failed to start process: {e}")
            return None

    def monitor_process_resources(self, file_path: Path, process: subprocess.Popen):
        """Simple resource monitoring"""
        if not self.config['monitor_resources']:
            return
        
        file_logger = self.create_file_logger(file_path)
        
        def resource_monitor():
            try:
                psutil_process = psutil.Process(process.pid)
                
                while process.poll() is None and not self.shutdown_requested:
                    try:
                        memory_mb = psutil_process.memory_info().rss / 1024 / 1024
                        
                        if memory_mb > self.config['memory_threshold_mb']:
                            file_logger.warning(f"⚠️ High memory usage: {memory_mb:.1f}MB")
                            
                            if self.config['restart_on_high_memory']:
                                file_logger.warning("🔄 Restarting due to high memory usage")
                                process.terminate()
                                break
                        
                        time.sleep(30)  # Check every 30 seconds
                        
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        break
                    except Exception:
                        break
                        
            except Exception:
                pass
        
        monitor_thread = threading.Thread(target=resource_monitor, daemon=True)
        monitor_thread.start()

    def monitor_process_output(self, file_path: Path, process: subprocess.Popen):
        """Monitor process output"""
        file_logger = self.create_file_logger(file_path)
        
        def read_stdout():
            while process.poll() is None and not self.shutdown_requested:
                try:
                    line = process.stdout.readline()
                    if line:
                        clean_line = line.strip()
                        file_logger.info(clean_line)
                except Exception as e:
                    file_logger.error(f"Error reading stdout: {e}")
                    break
                    
        def read_stderr():
            while process.poll() is None and not self.shutdown_requested:
                try:
                    line = process.stderr.readline()
                    if line:
                        clean_line = line.strip()
                        
                        if self.is_real_error(clean_line):
                            file_logger.error(f"🚨 {clean_line}")
                        else:
                            file_logger.info(clean_line)
                            
                except Exception as e:
                    file_logger.error(f"Error reading stderr: {e}")
                    break
        
        # Start monitoring threads
        stdout_thread = threading.Thread(target=read_stdout, daemon=True)
        stderr_thread = threading.Thread(target=read_stderr, daemon=True)
        
        stdout_thread.start()
        stderr_thread.start()

    def run_single_file(self, file_path: Path):
        """Run single file with simple restart logic"""
        file_logger = self.create_file_logger(file_path)
        self.restart_counts[str(file_path)] = 0
        
        while not self.shutdown_requested:
            start_time = time.time()
            
            process = self.start_file_process(file_path)
            if not process:
                file_logger.error("❌ Failed to start process")
                break
            
            self.current_processes[str(file_path)] = process
            
            # Start monitoring
            self.monitor_process_output(file_path, process)
            self.monitor_process_resources(file_path, process)
            
            # Wait for process completion
            return_code = process.wait()
            runtime = time.time() - start_time
            
            file_logger.info(f"📊 Process ended: code={return_code}, runtime={runtime:.1f}s")
            
            # Check if restart is needed
            if not self.should_restart(return_code):
                if return_code == 0:
                    file_logger.info("✅ Process completed successfully")
                else:
                    file_logger.info(f"🔄 No restart needed for exit code {return_code}")
                break
            
            # Restart logic
            self.restart_counts[str(file_path)] += 1
            restart_count = self.restart_counts[str(file_path)]
            
            if restart_count > self.config['max_restarts']:
                file_logger.error(f"🛑 Max restarts ({self.config['max_restarts']}) reached")
                break
            
            file_logger.warning(f"🔄 Restarting process (attempt #{restart_count})")
            
            # Calculate delay
            delay = self.calculate_restart_delay(file_path)
            file_logger.info(f"⏳ Restarting in {delay} seconds...")
            
            # Interruptible delay
            for _ in range(delay):
                if self.shutdown_requested:
                    return
                time.sleep(1)

    def stop_file_process(self, file_path: Path):
        """Stop process for file"""
        process = self.current_processes.get(str(file_path))
        if process and process.poll() is None:
            file_logger = self.create_file_logger(file_path)
            file_logger.info("🛑 Stopping process...")
            
            try:
                process.terminate()
                try:
                    process.wait(timeout=10)
                    file_logger.info("✅ Process stopped gracefully")
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    file_logger.warning("⚡ Process killed forcefully")
            except Exception as e:
                file_logger.error(f"❌ Error stopping process: {e}")
            finally:
                if str(file_path) in self.current_processes:
                    del self.current_processes[str(file_path)]

    def stop_all_processes(self):
        """Stop all processes"""
        self.logger.info("🛑 Stopping all processes...")
        for file_path in list(self.current_processes.keys()):
            self.stop_file_process(Path(file_path))

    def run_simple_management(self):
        """Main simple management loop"""
        self.logger.info("🤖 Starting Simple Bot Management")
        
        # Validate files
        valid_files = []
        for file_path_str in self.config['bot_files']:
            file_path = Path(file_path_str)
            if file_path.exists():
                valid_files.append(file_path)
                self.logger.info(f"✅ Bot file validated: {file_path}")
            else:
                self.logger.error(f"❌ Bot file not found: {file_path}")
        
        if not valid_files:
            self.logger.error("❌ No valid bot files found!")
            return
        
        try:
            if self.config['parallel_execution'] and len(valid_files) > 1:
                # Parallel execution
                self.logger.info(f"🔄 Running {len(valid_files)} files in parallel")
                futures = []
                
                for file_path in valid_files:
                    future = self.executor.submit(self.run_single_file, file_path)
                    futures.append(future)
                
                # Wait for completion
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"❌ Error in parallel execution: {e}")
            else:
                # Sequential execution
                for file_path in valid_files:
                    if self.shutdown_requested:
                        break
                    self.logger.info(f"🔄 Running file: {file_path}")
                    self.run_single_file(file_path)
                    
        except Exception as e:
            self.logger.error(f"💥 Critical error: {e}")
        finally:
            self.stop_all_processes()
            self.executor.shutdown(wait=True)

    def get_status(self) -> Dict[str, Any]:
        """Get current status"""
        active_processes = 0
        process_info = {}
        
        for file_path, process in self.current_processes.items():
            is_running = process.poll() is None
            if is_running:
                active_processes += 1
            
            process_info[Path(file_path).name] = {
                'running': is_running,
                'pid': process.pid,
                'restart_count': self.restart_counts.get(file_path, 0)
            }
        
        return {
            'timestamp': datetime.now().isoformat(),
            'total_files': len(self.config['bot_files']),
            'active_processes': active_processes,
            'processes': process_info,
            'shutdown_requested': self.shutdown_requested
        }


def main():
    """Main function for simple bot runner"""
    config = {
        'bot_files': [
            'main.py',
            # Добавьте другие файлы ботов здесь
        ],
        'max_restarts': 5,              # Максимум перезапусков
        'restart_delay': 10,            # Базовая задержка между перезапусками
        'max_restart_delay': 300,       # Максимальная задержка
        'monitor_resources': True,      # Мониторинг ресурсов
        'memory_threshold_mb': 400,     # Лимит памяти в МБ
        'restart_on_high_memory': True, # Перезапуск при превышении памяти
        'smart_log_filtering': True,    # Умная фильтрация логов
        'parallel_execution': True,     # Параллельное выполнение
    }
    
    try:
        runner = SimpleBotRunner(config)
        runner.run_simple_management()
    except KeyboardInterrupt:
        print("\n🛑 Shutdown requested")
    except Exception as e:
        print(f"💥 Critical error: {e}")
    finally:
        print("🤖 Simple Bot Runner terminated")


if __name__ == "__main__":
    main()
